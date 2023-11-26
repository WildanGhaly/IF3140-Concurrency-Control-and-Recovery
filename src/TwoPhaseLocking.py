class TwoPhaseLocking:
    def __init__(self, input_seq: str) -> None:
        self.SL_table               = {}
        self.XL_table               = {}
        self.seq                    = []
        self.timestamp              = []
        self.transaction_history    = []
        self.result                 = []
        self.queue                  = []

        self.process_input_sequence(input_seq)

    def process_input_sequence(self, input_seq: str):
        # Memproses urutan input dan memvalidasi setiap operasi.
        if input_seq.endswith(';'):
            input_seq = input_seq[:-1]

        input_seq = input_seq.split(';')
        for input in input_seq:
            input = input.strip()
            self.validate_and_store_operation(input)

        self.verify_commit_operations()
        self.verify_table_names()

    def validate_and_store_operation(self, input: str):
        # Validasi dan simpan operasi tunggal.
        operation = input[0]
        if operation in ('R', 'W'):
            self.store_read_write_operation(input, operation)
        elif operation == 'C':
            self.store_commit_operation(input)
        else:
            raise ValueError("Invalid operation detected")

    def store_read_write_operation(self, input: str, operation: str):
        # Simpan operasi baca/tulis.
        transaction_id = int(input[1])
        table_name = input[3]
        self.seq.append({"operation": operation, "transaction": transaction_id, "table": table_name})

        if transaction_id not in self.timestamp:
            self.timestamp.append(transaction_id)

    def store_commit_operation(self, input: str):
        # Simpan operasi commit.
        transaction_id = int(input[1])
        self.seq.append({"operation": 'C', "transaction": transaction_id})

        if transaction_id not in self.timestamp:
            raise ValueError("Transaction has no read or write operation")

    def verify_commit_operations(self):
        # Verifikasi bahwa setiap transaksi memiliki operasi commit.
        if len([x for x in self.seq if x["operation"] == 'C']) != len(set(self.timestamp)):
            raise ValueError("Missing commit operation")

    def verify_table_names(self):
        # Verifikasi bahwa nama tabel valid.
        invalid_tables = [x for x in self.seq if x["operation"] in ('R', 'W') and (len(x["table"]) != 1 or not x["table"].isalpha())]
        if invalid_tables:
            raise ValueError("Invalid table name")
        
    def XL(self, transaction: int, table: str) -> bool:
        if table in self.SL_table:
            if transaction in self.SL_table[table] and len(self.SL_table[table]) == 1:
                # remove the shared lock
                self.SL_table = {
                    k: v for k, v in self.SL_table.items() if v != transaction}
                self.XL_table[table] = transaction
                self.result.append(
                    {"operation": "UPL", "transaction": transaction, "table": table})
                self.transaction_history.append({"transaction" : transaction, "table": table, "operation": "UPL", "status": "Success"})
                return True
            else:
                return False
        else:
            if table in self.XL_table:
                if self.XL_table[table] == transaction:
                    return True
                else:
                    return False
            else:
                self.XL_table[table] = transaction
                self.result.append(
                    {"operation": "XL", "transaction": transaction, "table": table})
                self.transaction_history.append({"transaction" : transaction, "table": table, "operation": "XL", "status": "Success"})
                return True

    def SL(self, transaction: int, table: str) -> bool:
        if table in self.XL_table:
            if self.XL_table[table] == transaction:
                return True
            else:
                return False
        else:  
            if table in self.SL_table and transaction in self.SL_table[table]:
                return True
            else:  # Check if the table is locked by another shared lock
                # Add the current transaction to the shared lock table
                if table not in self.SL_table:
                    self.SL_table[table] = []
                self.SL_table[table].append(transaction)
                self.result.append(
                    {"operation": "SL", "transaction": transaction, "table": table})
                self.transaction_history.append({"transaction" : transaction, "table": table, "operation": "SL", "status": "Success"})
                return True
            
    def clear_XL(self, current: dict) -> None:
        if current["transaction"] in self.XL_table.values():
            table = [
                k for k, v in self.XL_table.items() if v == current["transaction"]]
            for t in table:
                self.result.append(
                    {"operation": "UL", "transaction": current["transaction"], "table": t})
                self.transaction_history.append({"transaction" : current["transaction"], "table": t, "operation": "UL", "status": "Success"})
            self.XL_table = {
                k: v for k, v in self.XL_table.items() if v != current["transaction"]}

    def clear_SL(self, current: dict) -> None:
        table = [
            k for k, v in self.SL_table.items() if v == current["transaction"]]
        for t in table:
            self.result.append(
                {"operation": "UL", "transaction": current["transaction"], "table": t})
            self.transaction_history.append({"transaction" : current["transaction"], "table": t, "operation": "UL", "status": "Success"})
        for k, v in self.SL_table.items():
            if current["transaction"] in v:
                v.remove(current["transaction"])
        self.SL_table = {
            k: v for k, v in self.SL_table.items() if v != []}

    

    def run_queue(self) -> None:
        while self.queue:
            transaction = self.queue.pop(0)
            # Check if the table is locked
            if self.XL(transaction["transaction"], transaction["table"]):
                self.result.append(transaction)
                self.transaction_history.append({"transaction" : transaction["transaction"], "table": transaction["table"], "operation": transaction["operation"], "status": "Success"})
            else:
                self.queue.insert(0, transaction)
                break

    def commit(self, current: dict) -> None:
        if current["transaction"] in [x["transaction"] for x in self.queue]:
            self.seq.insert(1, current)
        else:
            self.clear_SL(current)
            self.clear_XL(current)
            self.result.append(current)
            self.transaction_history.append({"transaction" : current["transaction"], "table": "-", "operation": "Commit", "status": "Commit"})

    def abort(self, current: dict) -> None:
        self.transaction_history.append({"transaction": current["transaction"], "table": current["table"], "operation": "Abort", "status": "Abort"})
        curr = [x for x in self.result if x["transaction"] == current["transaction"] and (
            x["operation"] == 'R' or x["operation"] == 'W')]
        self.result = [
            x for x in self.result if x["transaction"] != current["transaction"]]
        seq = [x for x in self.seq if x["transaction"] == current["transaction"]]
        self.seq = [
            x for x in self.seq if x["transaction"] != current["transaction"]]
        if current["transaction"] in self.XL_table.values():
            self.XL_table = {
                k: v for k, v in self.XL_table.items() if v != current["transaction"]}
        if current["transaction"] in [x for v in self.SL_table.values() for x in v]:
            for k, v in self.SL_table.items():
                if current["transaction"] in v:
                    v.remove(current["transaction"])
            self.SL_table = {
                k: v for k, v in self.SL_table.items() if v != []}

        self.seq.extend(curr)
        self.seq.append(current)
        self.seq.extend(seq)

    def wait_die(self, current: dict) -> None:
        if ((current["table"] in self.XL_table and self.timestamp.index(current["transaction"]) < self.timestamp.index(self.XL_table[current["table"]])) or
                (current["table"] in self.SL_table and all(self.timestamp.index(current["transaction"]) < self.timestamp.index(t) for t in self.SL_table[current["table"]] if t != current["transaction"]))):
            self.queue.append(current)
            self.transaction_history.append({"transaction": current["transaction"], "table": current["table"], "operation": current["operation"], "status": "Queue"})
        else:
            self.abort(current)

    def run(self) -> None:
        while self.seq:
            self.run_queue()
            index = next((i for i, x in enumerate(self.seq) if x["transaction"] not in [
                y["transaction"] for y in self.queue]), None)
            current = self.seq.pop(index)

            if current["operation"] == 'C':
                self.commit(current)
            elif current["operation"] == 'R' and self.SL(current["transaction"], current["table"]):
                self.result.append(current)
                self.transaction_history.append({"transaction": current["transaction"], "table": current["table"], "operation": current["operation"], "status": "Success"})
            elif current["operation"] == 'W' and self.XL(current["transaction"], current["table"]):
                self.result.append(current)
                self.transaction_history.append({"transaction": current["transaction"], "table": current["table"], "operation": current["operation"], "status": "Success"})
            else:
                self.wait_die(current)

    def result_string(self) -> None:
        res = ""
        for r in self.result:
            if r["operation"] == 'C':
                res += f"{r['operation']}{r['transaction']};"
            else:
                res += f"{r['operation']}{r['transaction']}({r['table']});"
        if res[-1] == ';':
            res = res[:-1]
        return res

    def history_string(self):
        str = ""
        for t in self.transaction_history:
            str += f"{t['operation']} {t['transaction']} {t['table'] if 'table' in t else ''}\n"
        return str
    
    def history_json(self):
        res = []
        for t in self.transaction_history:
            res.append({t["transaction"]: f'{t["operation"]}({t["table"]})'})
        return res

if __name__ == "__main__":
    try:
        lock = TwoPhaseLocking(input("Enter the seq: "))
        lock.run()
        print(lock.result_string())
        # print(lock.transaction_history)

    except (ValueError, IndexError) as e:
        print("Error: ", e)
        exit(1)


