from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG, INFO


class TimelyForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(TimelyForeignDataWrapper, self).__init__(options, columns)
        log_to_postgres(options, INFO)
        log_to_postgres(columns, INFO)
        self.columns = columns

    def execute(self, quals, columns):
        log_to_postgres(quals, INFO)
        log_to_postgres(columns, INFO)
        print(columns)

        for qual in quals:
            if qual.operator == '<':
                pass

        for res in ['0', '1', 'jim']:
            yield dict((col, res) for col in columns)
