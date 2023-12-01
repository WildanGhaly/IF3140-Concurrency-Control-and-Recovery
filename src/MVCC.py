from collections import deque

COMMIT_OPERATION    = "commit"
READ_OPERATION      = "read"
WRITE_OPERATION     = "write"

class MVCC:
    def __init__(self, input_sequence):
        self.counter                = 0
        self.version_table          = {}
        self.sequence               = deque([])
        self.input_sequence         = deque(input_sequence)
        self.transaction_counter    = [i for i in range(10)]
        self.result_string          = ""

    def get_max_version_index_by_write(self, item):
        max_w_timestamp = self.version_table[item][0]['timestamp'][1]
        max_index = 0
        for i in range(len(self.version_table[item])):
            if self.version_table[item][i]['timestamp'][1] > max_w_timestamp:
                max_w_timestamp = self.version_table[item][i]['timestamp'][1]
                max_index = i
        return max_index

    def write(self, tx, item):
        if item not in self.version_table.keys():
            self.version_table[item] = []
            self.version_table[item].append({'tx': tx, 'timestamp': (
                self.transaction_counter[tx], self.transaction_counter[tx]), 'version': self.transaction_counter[tx]})
            self.sequence.append({'tx': tx, 'item': item, 'action': 'write', 'timestamp': (
                self.transaction_counter[tx], self.transaction_counter[tx]), 'version': self.transaction_counter[tx]})
            temp = (f"T{tx}: W({item}) at version {self.transaction_counter[tx]}. Timestamp({item}): ({self.transaction_counter[tx]}, {self.transaction_counter[tx]}).")
            print(temp)
            self.result_string += temp + "; "
            
            self.counter += 1
        else:
            max_index = self.get_max_version_index_by_write(item)
            max_w_timestamp = self.version_table[item][max_index]['timestamp'][1]
            max_r_timestamp = self.version_table[item][max_index]['timestamp'][0]
            max_version = self.version_table[item][max_index]['version']

            if self.transaction_counter[tx] < max_r_timestamp:
                self.sequence.append({'tx': tx, 'item': item, 'action': 'write', 'timestamp': (
                    max_r_timestamp, self.transaction_counter[tx]), 'version': max_version})
                self.rollback(tx)
            elif self.transaction_counter[tx] == max_w_timestamp:
                self.version_table[item][max_index]['timestamp'] = (
                    max_r_timestamp, self.transaction_counter[tx])
                self.sequence.append({'tx': tx, 'item': item, 'action': 'write', 'timestamp': (
                    max_r_timestamp, self.transaction_counter[tx]), 'version': max_version})
                self.counter += 1
            else:
                self.version_table[item].append({'tx': tx, 'timestamp': (
                    max_r_timestamp, self.transaction_counter[tx]), 'version': self.transaction_counter[tx]})
                temp = (f"T{tx}: W({item}) at version {self.transaction_counter[tx]}. Timestamp({item}): ({max_r_timestamp}, {self.transaction_counter[tx]}).")
                print(temp)
                self.result_string += temp + "; "
                self.counter += 1


    def read(self, tx, item):
        if item not in self.version_table.keys():
            if ('tx', tx) not in self.version_table.items():
                self.version_table[item] = []
                self.version_table[item].append({'tx': tx, 'timestamp': (
                    self.transaction_counter[tx], 0), 'version': 0})
                self.sequence.append({'tx': tx, 'item': item, 'action': 'read', 'timestamp': (
                    self.transaction_counter[tx], 0), 'version': 0})
                temp = (f"T{tx}: R({item}) at version 0. Timestamp({item}): ({self.transaction_counter[tx]}, 0).")
                print(temp)
                self.result_string += temp + "; "
                self.counter += 1
            else:
                max_index = self.get_max_version_index_by_write(item)
                max_w_timestamp = self.version_table[item][max_index]['timestamp'][1]
                max_r_timestamp = self.version_table[item][max_index]['timestamp'][0]
                max_version = self.version_table[item][max_index]['version']

                if self.transaction_counter[tx] > max_r_timestamp:
                    self.version_table[item][max_index]['timestamp'] = (
                        self.transaction_counter[tx], max_w_timestamp)
                temp = (f"T{tx} R({item}) at version {max_version}. Timestamp({item}): {self.version_table[item][max_index]['timestamp']}.")
                print(temp)
                self.result_string += temp + "; "
                self.counter += 1
        else:
            max_index = self.get_max_version_index_by_write(item)
            max_w_timestamp = self.version_table[item][max_index]['timestamp'][1]
            max_r_timestamp = self.version_table[item][max_index]['timestamp'][0]
            max_version = self.version_table[item][max_index]['version']

            if self.transaction_counter[tx] > max_r_timestamp:
                self.version_table[item][max_index]['timestamp'] = (
                    self.transaction_counter[tx], max_w_timestamp)
            temp = (f"T{tx}: R({item}) at version {max_version}. Timestamp({item}): {self.version_table[item][max_index]['timestamp']}.")
            print(temp)
            self.result_string += temp + "; "
            self.counter += 1

    def rollback(self, tx):
        tx_sequence = []
        for i in range(len(self.sequence)):
            if self.sequence[i]['tx'] == tx and self.sequence[i]['action'] != 'aborted':
                tx_sequence.append(
                    {'tx': self.sequence[i]['tx'], 'item': self.sequence[i]['item'], 'action': self.sequence[i]['action']})
        for i in range(len(self.input_sequence)):
            if self.input_sequence[i]['tx'] == tx:
                tx_sequence.append(self.input_sequence[i])
                self.input_sequence.remove(self.input_sequence[i])
        for i in range(len(tx_sequence)):
            self.input_sequence.append(tx_sequence[i])
        self.sequence.append({'tx': tx, 'item': None, 'action': 'rollback'})
        self.transaction_counter[tx] = self.counter
        temp = (f"T{tx}: rolled back. Assigned new timestamp: {self.transaction_counter[tx]}.")
        print(temp)
        self.result_string += temp + "; "
        
    def print_sequence(self):
        for i in range(len(self.sequence)):
            if (self.sequence[i]['action'] == 'rollback'):
                temp = (f"T{self.sequence[i]['tx']}: rolled back.")
                print(temp)
                self.result_string += temp + "; "
            elif (self.sequence[i]['action'] != 'aborted'):
                temp = (self.sequence[i]['item'], self.sequence[i]['tx'],
                      self.sequence[i]['timestamp'], self.sequence[i]['version'])
                print(temp)
                self.result_string += temp + "; "

    def run(self):
        while len(self.input_sequence) > 0:
            current = self.input_sequence.popleft()
            if current['action'] == READ_OPERATION:
                self.read(current['tx'], current['item'])
            elif current['action'] == WRITE_OPERATION:
                self.write(current['tx'], current['item'])
            else:
                print("Invalid action.")

def parse_input(input_string):
    input_list = input_string.split(";")
    sequence = []

    for input_item in input_list:
        input_item = input_item.strip()
        if not input_item:
            continue

        try:
            action_type = input_item[0]
            tx = int(input_item[1])
            item = input_item[3]

            if action_type == "R":
                sequence.append({"action": READ_OPERATION, "tx": tx, "item": item})
            elif action_type == "W":
                sequence.append({"action": WRITE_OPERATION, "tx": tx, "item": item})
        except:
            print("Invalid input string")
            exit()

    return sequence

def main():
    input_string = input("Enter Concurrency Control Sequence: ")
    sequence = parse_input(input_string)

    mvcc = MVCC(sequence)
    mvcc.run()

if __name__ == "__main__":
    main()
    
