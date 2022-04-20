from queue import PriorityQueue
import numpy as np
import argparse


class CONST:
    CREATE_TXN = 1
    CREATE_BLOCK = 2
    RECEIVE_TXN = 3
    RECEIVE_BLOCK = 4


class ID:
    def __init__(self):
        self.bid = 0
        self.tid = 0

    def new_block_id(self):
        self.bid += 1
        return self.bid - 1

    def new_txn_id(self):
        self.tid += 1
        return self.tid - 1


class Event:
    def __init__(self, type, sender=-1, receiver=-1, data=None):
        self.type = type
        self.sender = sender
        self.receiver = receiver
        self.data = data

    def __lt__(self, other):
        return 0


class Utxo:
    def __init__(self, block_id, txn_index, op_index, owner, amount, target_shard):
        self.block_id = block_id
        self.txn_index = txn_index
        self.op_index = op_index
        self.owner = owner
        self.amount = amount
        self.target_shard = target_shard


class Block:
    def __init__(self, block_id, creator, creation_time, depth, parent_id, shard_id):
        self.block_id = block_id
        self.creator = creator
        self.creation_time = creation_time
        self.depth = depth
        self.parent_id = parent_id
        self.shard_id = shard_id
        self.txn_list = []
        self.size = 0


class BTNode:
    def __init__(self, block, parent_btnode):
        self.block = block
        self.parent_btnode = parent_btnode
        self.children = []


class Txn:
    def __init__(self, txn_id, target_shard, creator, sim):
        self.creator = creator
        self.txn_id = txn_id
        self.target_shard = target_shard
        self.inputs = []
        self.outputs = []
        self.size = 0
        self.creation_time = sim.cur_time
        self.confirmation_time_1 = 0
        self.confirmation_time_6 = 0

    def __lt__(self, other):
        return 0


class UtxoStore:
    def __init__(self):
        self.store = {}

    def search_utxo(self, block_id, txn_index, op_index, node_id):
        utuple = (block_id, txn_index, op_index)
        if utuple not in self.store:
            return None

        visibility, utxo = self.store[utuple]
        if node_id not in visibility:
            return None
        return utxo

    def add_utxo(self, utxo, node_id):
        utuple = (utxo.block_id, utxo.txn_index, utxo.op_index)
        if utuple not in self.store:
            visibility = set()
            visibility.add(node_id)
            self.store[utuple] = (visibility, utxo)
            r_utxo = utxo
        else:
            visibility, r_utxo = self.store[utuple]
            visibility.add(node_id)
        return r_utxo

    def remove_utxo(self, block_id, txn_index, op_index, node_id):
        utuple = (block_id, txn_index, op_index)
        if utuple in self.store:
            visibility, utxo = self.store[utuple]
            if node_id not in visibility:
                return None
            visibility.remove(node_id)
            if len(visibility) == 0:
                del self.store[utuple]
            return utxo
        return None


class TxnPool:
    def __init__(self):
        self.pq = PriorityQueue()
        self.pool = {}

    def add_txn(self, txn, priority):
        if txn.txn_id in self.pool:
            return
        self.pq.put((priority, txn))
        self.pool[txn.txn_id] = txn

    def remove_txn(self, txn_id):
        if txn_id in self.pool:
            del self.pool[txn_id]

    def pop_txn(self):
        while not self.pq.empty():
            priority, txn = self.pq.get()
            if txn.txn_id in self.pool:
                del self.pool[txn.txn_id]
                return priority, txn
        return None, None


class Connection:
    def __init__(self, nbr_id, sol_delay, speed):
        self.nbr_id = nbr_id
        self.sol_delay = sol_delay
        self.speed = speed


class Node:
    def __init__(self, node_id, sim):
        self.node_id = node_id
        self.sim = sim
        self.nbrs = {}
        self.block_id_to_btnode = {}
        self.mining_heads = {}
        self.txn_pools = [TxnPool() for _ in range(self.sim.SHARD_COUNT)]
        self.own_utxos = []
        self.received_blocks = set()
        self.received_txn = set()
        self.self_created_pending_txn = set()
        self.self_created_confirmed_txn = {}
        self.scheduled_next_block_generation_shard = -1

        self.max_depths = {}
        self.max_link_speed = int(np.random.uniform(5, 101)) * 1024 * 1024

    def setup_connections(self):
        count = int(np.random.uniform(2, 8))

        if count > self.sim.NODE_COUNT - 1:
            count = self.sim.NODE_COUNT - 1

        count -= len(self.nbrs)

        while count > 0:
            nbr_id = int(np.random.uniform(0, self.sim.NODE_COUNT))
            if nbr_id == self.node_id or nbr_id in self.nbrs:
                continue

            sol_delay = int(np.random.uniform(10, 501))
            speed = min(self.max_link_speed, self.sim.nodes[nbr_id].max_link_speed)

            self.nbrs[nbr_id] = Connection(nbr_id, sol_delay, speed)
            self.sim.nodes[nbr_id].nbrs[self.node_id] = Connection(self.node_id, sol_delay, speed)
            count -= 1

    def setup_genesis_blocks(self, Gblocks):
        for block in Gblocks:
            dummy_event = Event(-1, -1, -1, block)
            self.receive_block(dummy_event)

    def broadcast(self, type, size, data, received_from=-1):
        for nbr_id in self.nbrs:
            if nbr_id == received_from:
                continue
            conn = self.nbrs[nbr_id]
            delay = conn.sol_delay + ((size / conn.speed) * 1000) + (((96 * 1024) / conn.speed) * 1000)
            next_time = self.sim.cur_time + delay
            event = Event(type, self.node_id, nbr_id, data)
            self.sim.event_queue.put((next_time, event))

    def schedule_next_txn(self):
        next_time = int(self.sim.cur_time + np.random.exponential(self.sim.MEAN_TXN_TIME))
        self.sim.event_queue.put((next_time, Event(CONST.CREATE_TXN, self.node_id, self.node_id)))

    def create_txn(self):
        i_o_size = 60 * 8

        self.schedule_next_txn()

        txn = Txn(self.sim.ID.new_txn_id(), (21 * self.node_id) % self.sim.SHARD_COUNT, self.node_id, self.sim)

        sum = 0
        size = 0
        input_count = int(np.random.uniform(1, 3))
        while input_count > 0 and len(self.own_utxos) > 0:
            utxo = self.own_utxos.pop(int(np.random.uniform(0, len(self.own_utxos))))
            txn.inputs.append(
                (utxo.block_id, utxo.txn_index, utxo.op_index, utxo.owner, utxo.amount, utxo.target_shard))
            input_count -= 1
            sum += utxo.amount
            size += i_o_size

        if size == 0:
            size = i_o_size

        output_count = int(np.random.uniform(1, 3))
        
        if sum == 0:
            size += i_o_size
        
        while sum > 0 and output_count > 0:
            amount = int(np.random.uniform(1, sum + 1)) if output_count > 1 else sum
            output_count -= 1
            sum -= amount
            owner = int(np.random.uniform(0, self.sim.NODE_COUNT))
            target_shard = (21 * owner) % self.sim.SHARD_COUNT
            txn.outputs.append((owner, amount, target_shard))
            size += i_o_size

        txn.size = size

        self.self_created_pending_txn.add(txn.txn_id)
        self.sim.created_txn_count += 1

        self.sim.P(self.node_id, 'created txn T' + str(txn.txn_id))

        dummy_event = Event(-1, -1, -1, txn)
        self.receive_txn(dummy_event)

    def validate_txn(self, txn):
        for (bid, i_txn_idx, op_idx, owner, amount, target_shard) in txn.inputs:
            utxo = self.sim.utxo_store.search_utxo(bid, i_txn_idx, op_idx, self.node_id)
            if not utxo or utxo.target_shard != txn.target_shard:
                return False
        return True

    def receive_txn(self, event):
        txn = event.data

        if txn.txn_id in self.received_txn:
            return
        self.received_txn.add(txn.txn_id)

        self.sim.P(self.node_id, 'received txn T' + str(txn.txn_id) +
                   ' from N' + str(txn.creator))

        shard_id = txn.target_shard
        txn_pool = self.txn_pools[shard_id]
        txn_pool.add_txn(txn, self.sim.cur_time)
        self.broadcast(CONST.RECEIVE_TXN, txn.size, txn, event.sender)

    def schedule_block_generation(self):
        next_time = int(self.sim.cur_time + np.random.exponential(self.sim.MEAN_BLOCK_TIME))
        shard_id = int(np.random.uniform(0, self.sim.SHARD_COUNT))
        self.scheduled_next_block_generation_shard = shard_id
        self.sim.event_queue.put((next_time, Event(CONST.CREATE_BLOCK, self.node_id, self.node_id,
                                                   data=(shard_id, self.mining_heads[shard_id]))))

    def create_block(self, event):
        shard_id, prev_mining_head = event.data
        if self.mining_heads[shard_id] != prev_mining_head:
            return

        parent_id = self.mining_heads[shard_id]
        parent_btnode = self.block_id_to_btnode[parent_id]
        block_depth = parent_btnode.block.depth + 1

        block = Block(self.sim.ID.new_block_id(), self.node_id, self.sim.cur_time, block_depth, parent_id, shard_id)
        block.size = 128 * 8

        txn_pool = self.txn_pools[shard_id]

        while True:
            priority, txn = txn_pool.pop_txn()
            if not txn:
                break
            if not self.validate_txn(txn):
                continue
            if block.size + txn.size > self.sim.BLOCK_SIZE:
                txn_pool.add_txn(txn, priority)
                break
            block.txn_list.append(txn)
            block.size += txn.size

        self.sim.P(self.node_id, 'block B' + str(block.block_id) + ' created in shard S' +
                   str(block.shard_id) + ' parent B' + str(block.parent_id))

        dummy_event = Event(-1, -1, -1, block)
        self.receive_block(dummy_event)

    def switch_branch(self, shard_id, new_block_id, is_genesis=False):
        if is_genesis:
            self.add_block(self.block_id_to_btnode[new_block_id].block)
            self.mining_heads[shard_id] = new_block_id
            return

        old_head = self.block_id_to_btnode[self.mining_heads[shard_id]]
        new_head = self.block_id_to_btnode[new_block_id]
        old_branch = []
        new_branch = []

        while True:
            if new_head in old_branch:
                joint = new_head
                break
            else:
                new_branch.append(new_head)
                new_head = new_head.parent_btnode
            if old_head:
                old_branch.append(old_head)
                old_head = old_head.parent_btnode

        old_head = self.block_id_to_btnode[self.mining_heads[shard_id]]
        while old_head is not joint:
            self.remove_block(old_head.block)
            old_head = old_head.parent_btnode

        for btnode in reversed(new_branch):
            self.add_block(btnode.block)

        self.mining_heads[shard_id] = new_block_id
        if shard_id == self.scheduled_next_block_generation_shard:
            self.schedule_block_generation()

    def add_block(self, block):
        shard_id = block.shard_id
        txn_pool = self.txn_pools[shard_id]
        utxo_store = self.sim.utxo_store
        block_id = block.block_id

        for txn_idx, txn in enumerate(block.txn_list):

            if txn.txn_id in self.self_created_pending_txn:
                self.self_created_pending_txn.remove(txn.txn_id)
                self.self_created_confirmed_txn[txn.txn_id] = [0, shard_id, txn]

            txn_pool.remove_txn(txn.txn_id)

            for (bid, i_txn_idx, op_idx, owner, amount, target_shard) in txn.inputs:
                utxo_store.remove_utxo(bid, i_txn_idx, op_idx, self.node_id)

            for op_idx, (owner, amount, target_shard) in enumerate(txn.outputs):
                new_utxo = Utxo(block_id, txn_idx, op_idx, owner, amount, target_shard)
                new_utxo = utxo_store.add_utxo(new_utxo, self.node_id)
                if owner == self.node_id:
                    self.own_utxos.append(new_utxo)

        for txn_id, state in self.self_created_confirmed_txn.items():
            if state[1] == shard_id:
                txn = state[2]
                state[0] += 1
                if state[0] == 1:
                    self.sim.confirmed_txn_count += 1
                    self.sim.confirmed_txn_delay_1 += (self.sim.cur_time - txn.creation_time)
                    txn.confirmation_time_1 = self.sim.cur_time
                elif state[0] == 6:
                    self.sim.confirmed_txn_delay_6 += (self.sim.cur_time - txn.confirmation_time_1)
                    txn.confirmation_time_6 = self.sim.cur_time

    def remove_block(self, block):
        shard_id = block.shard_id
        txn_pool = self.txn_pools[shard_id]
        utxo_store = self.sim.utxo_store
        block_id = block.block_id

        for txn_idx, txn in enumerate(block.txn_list):
            txn_pool.add_txn(txn, self.sim.cur_time)

            for op_idx, (owner, amount, target_shard) in enumerate(txn.outputs):
                utxo = utxo_store.remove_utxo(block_id, txn_idx, op_idx, self.node_id)
                if utxo and owner == self.node_id and utxo in self.own_utxos:
                    self.own_utxos.remove(utxo)

            for (bid, i_txn_idx, op_idx, owner, amount, target_shard) in txn.inputs:
                new_utxo = Utxo(block_id, i_txn_idx, op_idx, owner, amount, target_shard)
                new_utxo = utxo_store.add_utxo(new_utxo, self.node_id)
                if owner == self.node_id:
                    self.own_utxos.append(new_utxo)

    def process_block(self, block):
        shard_id = block.shard_id
        parent_id = block.parent_id

        if parent_id == -1:  # Genesis block
            new_btnode = BTNode(block, None)
            self.block_id_to_btnode[block.block_id] = new_btnode
            self.switch_branch(shard_id, block.block_id, True)
            return True

        if parent_id not in self.block_id_to_btnode:
            return False

        parent_btnode = self.block_id_to_btnode[parent_id]
        if (parent_btnode.block.depth + 1) != block.depth:
            return False

        new_btnode = BTNode(block, parent_btnode)
        self.block_id_to_btnode[block.block_id] = new_btnode
        parent_btnode.children.append(new_btnode)

        if self.block_id_to_btnode[self.mining_heads[shard_id]].block.depth < block.depth:
            self.switch_branch(shard_id, block.block_id)
        return True

    def receive_block(self, event):
        block = event.data

        if block.block_id in self.received_blocks:
            return
        self.received_blocks.add(block.block_id)

        if self.process_block(event.data):
            self.broadcast(CONST.RECEIVE_BLOCK, block.size, block, event.sender)

        if block.creator == -1:
            self.sim.P(self.node_id, 'Setting Genesis block B' + str(block.block_id) +
                       ' in shard S' + str(block.shard_id))
        else:
            self.sim.P(self.node_id, 'block B' + str(block.block_id) +
                       ' received from N' + str(block.creator) + ' in shard S' +
                       str(block.shard_id))


class Sim:
    def __init__(self, args):

        self.NODE_COUNT = args.N
        self.BLOCK_SIZE = args.BS * 1024 * 8
        self.SHARD_COUNT = args.S
        self.TXN_PER_SEC = 15 * self.SHARD_COUNT  # number of txns created per sec in the system
        self.MEAN_TXN_TIME = (self.NODE_COUNT / self.TXN_PER_SEC) * 1000
        self.INTERARRIVAL_TIME = args.IA * 1000
        self.MEAN_BLOCK_TIME = (self.INTERARRIVAL_TIME * self.NODE_COUNT) / self.SHARD_COUNT

        self.ID = ID()

        self.utxo_store = UtxoStore()
        self.nodes = []
        for id in range(self.NODE_COUNT):
            self.nodes.append(Node(id, self))

        self.event_queue = PriorityQueue()
        self.cur_time = 0
        self.end_time = args.duration * 60 * 1000
        self.created_txn_count = 0
        self.confirmed_txn_count = 0
        self.confirmed_txn_delay_1 = 0
        self.confirmed_txn_delay_6 = 0

    def P(self, node_id, msg):
        print("Time : {time:.3f} | N{node_id} | {msg}".format(time=self.cur_time / 1000, node_id=node_id, msg=msg))

    def create_genesis_blocks(self):
        blocks = []
        for shard_id in range(self.SHARD_COUNT):
            block = Block(self.ID.new_block_id(), -1, 0, 0, -1, shard_id)
            txn = Txn(self.ID.new_txn_id(), shard_id, -1, self)
            for owner in range(self.NODE_COUNT):
                for _ in range(21):
                    txn.outputs.append((owner, 1, (21 * owner) % self.SHARD_COUNT))
            block.txn_list.append(txn)
            blocks.append(block)
        return blocks

    def setup(self):
        Gblocks = self.create_genesis_blocks()

        for node in self.nodes:
            node.setup_connections()
            node.setup_genesis_blocks(Gblocks)
            node.schedule_next_txn()
            node.schedule_block_generation()

    def run(self):
        self.setup()

        while True:

            time, event = self.event_queue.get()
            if time > self.end_time:
                break
            self.cur_time = time

            if event.type == CONST.CREATE_TXN:
                self.nodes[event.receiver].create_txn()
            elif event.type == CONST.CREATE_BLOCK:
                self.nodes[event.receiver].create_block(event)
            elif event.type == CONST.RECEIVE_TXN:
                self.nodes[event.receiver].receive_txn(event)
            elif event.type == CONST.RECEIVE_BLOCK:
                self.nodes[event.receiver].receive_block(event)

        print("Confirmed {confirmed} txns out of {created}".format(confirmed=self.confirmed_txn_count,
                                                                   created=self.created_txn_count))
        tps = self.confirmed_txn_count / (self.end_time / 1000)
        tpsps = tps / self.SHARD_COUNT
        print("Total Throughput : {tps:.2f} txns per sec ({tpsps:.2f} per shard)".format(tps=tps,
                                                                                         tpsps=tpsps))
        avg_1confirmation_time = (self.confirmed_txn_delay_1 / self.confirmed_txn_count) / 1000
        avg_6confirmation_time = (self.confirmed_txn_delay_6 / self.confirmed_txn_count) / 1000
        print("Avg. 1st confirmation delay : {cnft:.2f} sec".format(cnft=avg_1confirmation_time))
        print("Avg. time between 1st and 6th confirmation : {cnft:.2f} sec".format(cnft=avg_6confirmation_time))
        print("Avg. total delay (from txn creation to 6th confirmation) : {td:.2f} sec".format(
            td=avg_1confirmation_time + avg_6confirmation_time))

        output_file_name = "N_{nodes}_S_{shards}_BS_{block_size}_IA_{interarrival}_duration_{duration}.wcdata".format(
            nodes=args.N, shards=args.S, block_size=args.BS, interarrival=args.IA, duration=args.duration)
        f = open(output_file_name, 'w')
        f.write(output_file_name+'\n')
        f.write("Confirmed / Created txns | {confirmed} / {created}\n".format(confirmed=self.confirmed_txn_count,
                                                                            created=self.created_txn_count))
        f.write("Total Throughput (txns per sec) | {tps:.2f}\n".format(tps=tps))
        f.write("Per Shard Throughput (txns per sec) | {tpsps:.2f}\n".format(tpsps=tpsps))
        f.write("Avg. 1st confirmation delay (secs) | {cnft:.2f}\n".format(cnft=avg_1confirmation_time))
        f.write("Avg. time between 1st and 6th confirmation (secs) | {cnft:.2f}\n".format(cnft=avg_6confirmation_time))
        f.write("Avg. total delay (from txn creation to 6th confirmation) (secs) | {td:.2f}\n".format(
            td=avg_1confirmation_time + avg_6confirmation_time))
        f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--N', help='Node count', default=64, type=int)
    parser.add_argument('--S', help='Shard count', default=4, type=int)
    parser.add_argument('--BS', help='Block Size in KB', default=32, type=int)
    parser.add_argument('--IA', help='Interarrival time in seconds', default=15, type=int)
    parser.add_argument('--duration', help='Simulation duration in minutes', default=15, type=int)

    args = parser.parse_args()

    sim = Sim(args)
    sim.run()
