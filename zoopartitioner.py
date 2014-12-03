import logging
import os
import socket
from functools import partial

from kazoo.exceptions import KazooException
from kazoo.protocol.states import KazooState
from kazoo.recipe.partitioner import PartitionState

import time

log = logging.getLogger(__name__)


class PatientChildrenKeyWatch(object):
    def __init__(self, client, paths, keys, time_boundary=30):
        self.client = client
        self.paths = paths
        self.keys = keys
        self.children = []
        self.values = []
        self.time_boundary = time_boundary
        self.children_changed = client.handler.event_object()
        self.async_result = None
        self._stopped = True
        self._suspended = False
        self.client.add_listener(self._session_watcher)

    def _get_children(self):
        children, values = [], []
        for path in self.paths:
            children.append(self.client.retry(self.client.get_children, path, self._children_watcher))
        for key in self.keys:
            values.append(self.client.retry(self.client.get, key, self._children_watcher)[0])
        return children, values

    def _check_children(self):
        """
            Check if children where changed during suspended connection
        """
        def froze_list(l):
            return frozenset(frozenset(child) for child in l)
        children, values = self._get_children()
        if froze_list(children) != froze_list(self.children) or frozenset(values) != frozenset(self.values):
            self._children_watcher()

    def start(self):
        self.asy = asy = self.client.handler.async_result()
        self.client.handler.spawn(self._inner_start)
        return asy

    def _inner_start(self):
        try:
            while True:
                self.async_result = self.client.handler.async_result()
                self.children, self.values = self._get_children()
                self.client.handler.sleep_func(self.time_boundary)
                if self.children_changed.is_set():
                    self.children_changed.clear()
                else:
                    break
            self._suspended = False
            self._stopped = False
            self.asy.set((self.children, self.values, self.async_result))
        except Exception as exc:
            self.asy.set_exception(exc)

    def _children_watcher(self, event=None):
        self.children_changed.set()
        if not self._stopped:
            self.async_result.set(time.time())
            self._stopped = True

    def _session_watcher(self, state):
        if state in (KazooState.LOST, KazooState.SUSPENDED):
            self._suspended = True
        elif (state == KazooState.CONNECTED):
            if (self._suspended and not self._stopped):
                self.client.handler.spawn(self._check_children)
            self._suspended = False



class ZooKeyPartitioner(object):
    def __init__(self, client, path, watch_obj=None, result_func=None, partitions_set=None, partition_func=None,
                 identifier=None, time_boundary=30):
        self.state = PartitionState.ALLOCATING

        if (watch_obj and partitions_set) or (not watch_obj and not partitions_set):
            raise Exception('need or watch_obj or partitions_set, only one not both')
        if (result_func and partitions_set) or (not result_func and not partitions_set):
            raise Exception('need or result_func or partitions_set, only one not both')
        if partitions_set:
            watch_obj = lambda _client, _path: PatientChildrenKeyWatch(_client, [_path], [], time_boundary)
            result_func = lambda _result: (partitions_set, _result[2])
        if not callable(result_func) or not callable(watch_obj):
            raise Exception('result_func and watch_obj must be callable')

        self._client = client
        self._path = path
        self._watch_obj = watch_obj
        self._result_func = result_func
        self._partition_set = []
        self._partition_func = partition_func or self._partitioner
        self._identifier = identifier or '%s-%s' % (
            socket.getfqdn(), os.getpid())
        self._locks = []
        self._lock_path = '/'.join([path, 'locks'])
        self._party_path = '/'.join([path, 'party'])

        self._acquire_event = client.handler.event_object()

        # Create basic path nodes
        client.ensure_path(path)
        client.ensure_path(self._lock_path)
        client.ensure_path(self._party_path)

        # Join the party
        self._party = client.ShallowParty(self._party_path,
                                          identifier=self._identifier)
        self._party.join()

        self._was_allocated = False
        self._state_change = client.handler.rlock_object()
        client.add_listener(self._establish_sessionwatch)

        # Now watch the party and set the callback on the async result
        # so we know when we're ready
        self._children_updated = False
        self._child_watching(self._allocate_transition, async=True)

    def __iter__(self):
        """Return the partitions in this partition set"""
        for partition in self._partition_set:
            yield partition

    @property
    def failed(self):
        """Corresponds to the :attr:`PartitionState.FAILURE` state"""
        return self.state == PartitionState.FAILURE

    @property
    def release(self):
        """Corresponds to the :attr:`PartitionState.RELEASE` state"""
        return self.state == PartitionState.RELEASE

    @property
    def allocating(self):
        """Corresponds to the :attr:`PartitionState.ALLOCATING`
        state"""
        return self.state == PartitionState.ALLOCATING

    @property
    def acquired(self):
        """Corresponds to the :attr:`PartitionState.ACQUIRED` state"""
        return self.state == PartitionState.ACQUIRED

    def wait_for_acquire(self, timeout=30):
        """Wait for the set to be partitioned and acquired

        :param timeout: How long to wait before returning.
        :type timeout: int

        """
        self._acquire_event.wait(timeout)

    def release_set(self):
        """Call to release the set

        This method begins the step of allocating once the set has
        been released.

        """
        self._release_locks()
        if self._locks:  # pragma: nocover
            # This shouldn't happen, it means we couldn't release our
            # locks, abort
            self._fail_out()
            return
        else:
            with self._state_change:
                if self.failed:
                    return
                self.state = PartitionState.ALLOCATING
        self._child_watching(self._allocate_transition, async=True)

    def finish(self):
        """Call to release the set and leave the party"""
        self._release_locks()
        self._fail_out()

    def _fail_out(self):
        with self._state_change:
            self.state = PartitionState.FAILURE
        if self._party.participating:
            try:
                self._party.leave()
            except KazooException:  # pragma: nocover
                pass

    def _allocate_transition(self, result):
        """Called when in allocating mode, and the children settled"""
        # Did we get an exception waiting for children to settle?
        if result.exception:  # pragma: nocover
            self._fail_out()
            return

        self._set, async_result = self._result_func(result.get())
        self._children_updated = False

        # Add a callback when children change on the async_result
        def updated(result):
            with self._state_change:
                if self.acquired:
                    self.state = PartitionState.RELEASE
            self._children_updated = True

        async_result.rawlink(updated)

        # Split up the set
        self._partition_set = self._partition_func(
            self._identifier, list(self._party), self._set)

        # Proceed to acquire locks for the working set as needed
        for member in self._partition_set:
            if self._children_updated or self.failed:
                # Still haven't settled down, release locks acquired
                # so far and go back
                return self._abort_lock_acquisition()

            lock = self._client.Lock(self._lock_path + '/' +
                                     str(member), identifier=self._identifier)
            try:
                lock.acquire()
            except KazooException:  # pragma: nocover
                return self.finish()
            self._locks.append(lock)

        # All locks acquired! Time for state transition, make sure
        # we didn't inadvertently get lost thus far
        with self._state_change:
            if self.failed:  # pragma: nocover
                return self.finish()
            self.state = PartitionState.ACQUIRED
            self._acquire_event.set()

    def _release_locks(self):
        """Attempt to completely remove all the locks"""
        self._acquire_event.clear()
        for lock in self._locks[:]:
            try:
                lock.release()
            except KazooException:  # pragma: nocover
                # We proceed to remove as many as possible, and leave
                # the ones we couldn't remove
                pass
            else:
                self._locks.remove(lock)

    def _abort_lock_acquisition(self):
        """Called during lock acquisition if a party change occurs"""
        self._partition_set = []
        self._release_locks()
        if self._locks:
            # This shouldn't happen, it means we couldn't release our
            # locks, abort
            self._fail_out()
            return
        return self._child_watching(self._allocate_transition)

    def _child_watching(self, func=None, async=False):
        """Called when children are being watched to stabilize

        This actually returns immediately, child watcher spins up a
        new thread/greenlet and waits for it to stabilize before
        any callbacks might run.

        """
        watcher = self._watch_obj(self._client, self._party_path)
        asy = watcher.start()
        if func is not None:
            # We spin up the function in a separate thread/greenlet
            # to ensure that the rawlink's it might use won't be
            # blocked
            if async:
                func = partial(self._client.handler.spawn, func)
            asy.rawlink(func)
        return asy

    def _establish_sessionwatch(self, state):
        """Register ourself to listen for session events, we shut down
        if we become lost"""
        with self._state_change:
            # Handle network partition: If connection gets suspended,
            # change state to ALLOCATING if we had already ACQUIRED. This way
            # the caller does not process the members since we could eventually
            # lose session get repartitioned. If we got connected after a suspension
            # it means we've not lost the session and still have our members. Hence,
            # restore to ACQUIRED
            if state == KazooState.SUSPENDED:
                if self.state == PartitionState.ACQUIRED:
                    self._was_allocated = True
                    self.state = PartitionState.ALLOCATING
            elif state == KazooState.CONNECTED:
                if self._was_allocated:
                    self._was_allocated = False
                    self.state = PartitionState.ACQUIRED

        if state == KazooState.LOST:
            self._client.handler.spawn(self._fail_out)
            return True

    def _partitioner(self, identifier, members, partitions):
        # Ensure consistent order of partitions/members
        all_partitions = sorted(partitions)
        workers = sorted(members)
        i = workers.index(identifier)
        logging.warn('for partitioners {0} and workers {1} worker {2}::{3} choose {4}'.format(all_partitions, workers, i, identifier, all_partitions[i::len(workers)]))
        # Now return the partition list starting at our location and
        # skipping the other workers
        return all_partitions[i::len(workers)]


def partitioner_wrapper(partitioner_obj):
    pos = 0
    active_partitions = []
    while True:
        if partitioner_obj.failed:
            raise Exception("Lost or unable to acquire partition")
        elif partitioner_obj.release:
            partitioner_obj.release_set()
            active_partitions = []
        elif partitioner_obj.acquired:
            if not active_partitions:
                active_partitions = list(p for p in partitioner_obj)
                logging.warn(str(active_partitions))
                if not active_partitions:
                    time.sleep(10)
                    continue
            if pos >= len(active_partitions):
                pos = 0
            yield active_partitions[pos]
            pos = pos + 1
        elif partitioner_obj.allocating:
            partitioner_obj.wait_for_acquire()
