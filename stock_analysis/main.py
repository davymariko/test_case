import pandas as pd
import sqlite3


class Stock:
    DB_FILE = "data/stock_data.db"

    def __init__(self):
        self.conn = sqlite3.connect(self.DB_FILE)
        self.cursor = self.conn.cursor()

    def load_csv_into_db(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                date TEXT,
                terminal_id TEXT,
                pos_id TEXT,
                transaction_amount REAL,
                stock_balance REAL
            )
        """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_levels (
                date TEXT,
                terminal_id TEXT,
                pos_id TEXT,
                stock_balance REAL
            )
        """)

        self.conn.commit()

        transactions = pd.read_csv("/app/data/transactions.csv")
        stock_levels = pd.read_csv("/app/data/stock-level.csv")

        transactions.to_sql("transactions", self.conn, if_exists="replace", index=False)
        stock_levels.to_sql("stock_levels", self.conn, if_exists="replace", index=False)

    def read_data_from_db(self):
        self.transactions = pd.read_sql("SELECT * FROM transactions", self.conn)
        self.stock_levels = pd.read_sql("SELECT * FROM stock_levels", self.conn)

        self.transactions['date'] = pd.to_datetime(self.transactions['date'])
        self.stock_levels['date'] = pd.to_datetime(self.stock_levels['date'])

    def define_out_stock(self):
        self.transactions['date_only'] = self.transactions['date'].dt.date
        avg_daily_sales = self.transactions.groupby(['pos_id', 'date_only'])['transaction_amount'].sum().groupby('pos_id').mean()

        self.stock_levels['out_of_stock_limit'] = self.stock_levels['pos_id'].map(lambda x: avg_daily_sales.get(x, 0) * 1.5)
        self.stock_levels['out_of_stock'] = self.stock_levels['stock_balance'] < self.stock_levels['out_of_stock_limit']

        self.stock_levels = self.stock_levels.sort_values(['pos_id', 'date'])
        self.stock_levels['previous_stock'] = self.stock_levels.groupby('pos_id')['out_of_stock'].shift(1)
        self.stock_levels['stock_change'] = (self.stock_levels['previous_stock'] == False) & (self.stock_levels['out_of_stock'] == True)

        recurring_out_of_stock_counts = self.stock_levels.groupby('pos_id')['stock_change'].sum()

        self.stock_levels['duration_out_of_stock'] = self.stock_levels.groupby('pos_id')['date'].diff().dt.total_seconds() / 3600
        self.stock_levels['long_outage'] = self.stock_levels['duration_out_of_stock'] >= 4

        recurrent_out_of_stock = (recurring_out_of_stock_counts >= 2) & (self.stock_levels.groupby('pos_id')['long_outage'].any())

        print("Recurrently Out of Stock POS:")
        print(recurrent_out_of_stock)
        print("Magic stuuuffff")

    def close_connection(self):
        self.conn.close()


if __name__ == "__main__":
    stock_checker = Stock()
    stock_checker.load_csv_into_db()
    stock_checker.read_data_from_db()
    stock_checker.define_out_stock()
    stock_checker.close_connection()
