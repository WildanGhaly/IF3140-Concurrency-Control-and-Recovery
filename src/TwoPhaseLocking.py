class TwoPhaseLocking:
    def __init__(self, input_string):
        self.operations = self.parse_input(input_string)
        self.lock_table = {}
        self.transaction_status = {}

    def parse_input(self, input_string):
        return [op.split('(') if '(' in op else [op] for op in input_string.split(';')]

    def can_acquire_lock(self, transaction, item, lock_type):
        if item not in self.lock_table:
            return True
        if self.lock_table[item]["lock_type"] == "read" and lock_type == "read":
            return True
        if self.lock_table[item]["lock_type"] == "read" and lock_type == "write":
            return self.lock_table[item]["holders"] == {transaction}
        return False

    def acquire_lock(self, transaction, item, lock_type):
        if self.can_acquire_lock(transaction, item, lock_type):
            if item not in self.lock_table:
                self.lock_table[item] = {"lock_type": lock_type, "holders": {transaction}}
            else:
                self.lock_table[item]["holders"].add(transaction)
                if lock_type == "write":
                    self.lock_table[item]["lock_type"] = lock_type
            return True
        return False

    def release_locks(self, transaction):
        for item, lock_info in list(self.lock_table.items()):
            if transaction in lock_info["holders"]:
                lock_info["holders"].remove(transaction)
                if not lock_info["holders"]:
                    del self.lock_table[item]

    def run(self):
        for op in self.operations:
            if len(op) == 1:  # Handle commit operation
                transaction = int(op[0][1:])
                self.release_locks(transaction)
                self.transaction_status[transaction] = "committed"
                print(f"Transaction {transaction} committed")
                continue

            transaction, item = int(op[0][1:]), op[1][:-1]
            lock_type = 'write' if op[0][0] == 'W' else 'read'

            if transaction not in self.transaction_status:
                self.transaction_status[transaction] = "growing"

            if lock_type == "write" and self.transaction_status[transaction] == "shrinking":
                return f"Transaction {transaction} cannot acquire new locks in the shrinking phase."

            if not self.acquire_lock(transaction, item, lock_type):
                return f"Deadlock detected. Transaction {transaction} cannot acquire {lock_type} lock on {item}."

            print(f"Transaction {transaction} performs {op[0][0]} on {item}")

            if lock_type == "write":
                self.transaction_status[transaction] = "shrinking"

        return "All operations executed successfully."

# Cara menjalankan program:
input_string = "R1(A);R2(A);C1;W2(A)"
tp_locking = TwoPhaseLocking(input_string)
print(tp_locking.run())
