class TableJoiner:
    def __init__(self, hive_context, query):
        self.query = query
        self.hive_context = hive_context

    def join_tables(self):
        df = self.hive_context.sql(self.query)
        return df
