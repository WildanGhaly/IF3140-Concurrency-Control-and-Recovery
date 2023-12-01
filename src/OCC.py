import math

class Transaction:
    def __init__(self, tx_id):
        self.tx_id      = tx_id
        self.reads      = []
        self.writes     = []
        self.timestamps = {
            "start": math.inf,
            "validation": math.inf,
            "finish": math.inf
        }

    def __str__(self):
        read_set_str    = ", ".join(self.reads)
        write_set_str   = ", ".join(self.writes)

        return (
            f"Transaction {self.tx_num}:\n"
            f"\tRead Set:{read_set_str}\n"
            f"\tWrite Set:{write_set_str}\n"
            f"\tTimestamps:{self.timestamps}"
        )

class OCC:
    COMMIT_OPERATION                = 'C'
    READ_OPERATION                  = 'R'
    WRITE_OPERATION                 = 'W'

    def __init__(self, input_sequence: str) -> None:
        self.current_timestamp      = 0
        self.timestamp              = []
        self.sequence               = []
        self.transactions           = {}
        self.result                 = []
        self.history_transaction    = []
        self.rollback_transactions  = []

        try:
            if (input_sequence.endswith(';')):
                input_sequence = input_sequence[:-1]

            self.parse_input_sequence(input_sequence)
            self.validate_operations()

        except ValueError as e:
            raise ValueError(str(e))

    def parse_input_sequence(self, input_sequence: str) -> None:
        for operation_str in input_sequence.split(';'):
            operation_str = operation_str.strip()
            self.parse_operation(operation_str)

    def parse_operation(self, operation_str: str) -> None:
        operation = operation_str[0]
        if (operation in {self.READ_OPERATION, self.WRITE_OPERATION}):
            self.handle_read_write_operation(operation, operation_str)
        elif (operation == self.COMMIT_OPERATION):
            self.handle_commit_operation(operation_str)
        else:
            raise ValueError("Invalid operation detected")

    def handle_read_write_operation(self, operation: str, operation_str: str) -> None:
        transaction_id = int(operation_str[1])
        table_name = operation_str[3]

        self.sequence.append({"operation": operation, "transaction": transaction_id, "table": table_name})

        if (transaction_id not in self.timestamp):
            self.timestamp.append(transaction_id)

    def handle_commit_operation(self, operation_str: str) -> None:
        transaction_id = int(operation_str[1])
        self.sequence.append({"operation": self.COMMIT_OPERATION, "transaction": transaction_id})

    def validate_operations(self) -> None:
        if (len([x for x in self.sequence if x["operation"] == self.COMMIT_OPERATION]) != len(set(self.timestamp))):
            raise ValueError("Missing commit operation")

        if (any(len(x["table"]) != 1 or not x["table"].isalpha() for x in self.sequence if x["operation"] in {self.READ_OPERATION, self.WRITE_OPERATION})):
            raise ValueError("Invalid table name")

    def read(self, cmd) -> None:
        self.current_timestamp += 1
        transaction_id = cmd['transaction']

        if (cmd['table'] not in self.transactions[transaction_id].reads):
            self.transactions[transaction_id].reads.append(cmd['table'])

        self.history_transaction.append(
            {"operation": cmd['operation'], "transaction": transaction_id, "table": cmd['table'], "status": "success"}
        )

    def tempwrite(self, cmd) -> None:
        self.current_timestamp += 1
        transaction_id = cmd['transaction']

        if (cmd['table'] not in self.transactions[transaction_id].writes):
            self.transactions[transaction_id].writes.append(cmd['table'])

        self.history_transaction.append(
            {"operation": cmd['operation'], "transaction": transaction_id, "table": cmd['table'], "status": "success"}
        )

    def validate(self, cmd) -> None:
        self.current_timestamp += 1
        transaction_id = cmd['transaction']
        self.transactions[transaction_id].timestamps['validation'] = self.current_timestamp
        valid = True

        for other_tx_id in self.transactions.keys():
            if (other_tx_id != transaction_id):
                validation_timestamp_ti = self.transactions[other_tx_id].timestamps['validation']
                finish_timestamp_ti     = self.transactions[other_tx_id].timestamps['finish']
                start_timestamp_tj      = self.transactions[transaction_id].timestamps['start']
                validation_timestamp_tj = self.transactions[transaction_id].timestamps['validation']
                if (validation_timestamp_ti < validation_timestamp_tj):
                    if (finish_timestamp_ti < start_timestamp_tj):
                        pass
                    elif (finish_timestamp_ti != math.inf and (start_timestamp_tj < finish_timestamp_ti and finish_timestamp_ti < validation_timestamp_tj)):
                        write_set_ti            = self.transactions[other_tx_id].writes
                        read_set_tj             = self.transactions[transaction_id].reads
                        is_element_intersect    = False
                        for v in write_set_ti:
                            if v in read_set_tj:
                                is_element_intersect = True
                                break
                        if is_element_intersect:
                            valid = False
                            break
                    else:
                        valid = False
                        break
        if valid:
            self.commit(cmd)
        else:
            self.handle_aborted_transaction(cmd, transaction_id)

    def handle_aborted_transaction(self, cmd, transaction_id) -> None:
        print(f"Transaction {transaction_id} is aborted")
        self.rollback_transactions.append(transaction_id)
        self.history_transaction.append(
            {"operation": cmd['operation'], "transaction": transaction_id, "status": "aborted"}
        )

    def commit(self, cmd) -> None:
        self.current_timestamp += 1
        transaction_id = cmd['transaction']
        self.transactions[transaction_id].timestamps['finish'] = self.current_timestamp

        for cmds in self.sequence:
            if cmds['transaction'] == transaction_id:
                self.result.append(cmds)

        self.history_transaction.append(
            {"operation": cmd['operation'], "transaction": transaction_id, "status": "commit"}
        )

        self.result.append(
            {"operation": cmd['operation'], "transaction": transaction_id}
        )
    
    def run_rollbacks(self) -> None:
        while self.rollback_transactions:
            self.current_timestamp += 1
            tx_id = self.rollback_transactions.pop(0)
            self.reset_transaction_attr(tx_id)
            self.replay_transaction_commands(tx_id)

    def reset_transaction_attr(self, tx_id) -> None:
        cmd             = self.transactions[tx_id]
        cmd.reads       = []
        cmd.writes      = []
        cmd.timestamps  = {"start": self.current_timestamp, "validation": math.inf, "finish": math.inf}

    def replay_transaction_commands(self, tx_id) -> None:
        cmd_sequence = [cmds for cmds in self.sequence if cmds['transaction'] == tx_id]
        
        for cmds in cmd_sequence:
            if (cmds['operation']    == self.READ_OPERATION):
                self.read(cmds)
            elif (cmds['operation']  == self.WRITE_OPERATION):
                self.tempwrite(cmds)
            elif (cmds['operation']  == self.COMMIT_OPERATION):
                self.validate(cmds)

            self.current_timestamp += 1

        self.current_timestamp += 1

    def run(self) -> None:
        for cmd in self.sequence:
            self.create_transaction(cmd)

            if (cmd['operation']    == self.READ_OPERATION):
                self.read(cmd)
            elif (cmd['operation']  == self.WRITE_OPERATION):
                self.tempwrite(cmd)
            elif (cmd['operation']  == self.COMMIT_OPERATION):
                self.validate(cmd)

            self.current_timestamp += 1

        self.run_rollbacks()

    def create_transaction(self, cmd) -> None:
        transaction_id = cmd['transaction']
        if (transaction_id not in self.transactions):
            self.transactions[transaction_id] = Transaction(transaction_id)
            self.transactions[transaction_id].timestamps['start'] = self.current_timestamp

    def __str__(self):

        res = ""
        for cmd in self.history_transaction:
            if cmd['status'] == 'success':
                res += f"{cmd['operation']}{cmd['transaction']}({cmd['table']})\n"
            elif cmd['status'] == 'commit':
                res += f"{cmd['operation']}{cmd['transaction']} - commit\n"
            elif cmd['status'] == 'aborted':
                res += f"{cmd['operation']}{cmd['transaction']} - aborted\n"
        return res


if __name__ == '__main__':
    try:
        occ = OCC(input("Enter Concurrency Control Sequence: "))
        occ.run()
        print(occ)
    except Exception as e:
        print("Error: ", e)
