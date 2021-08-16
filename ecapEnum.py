import enum


class Operation(enum.Enum):
    READ = "READ"
    MODIFY = "MODIFY"
    CREATE = "CREATE"
    DELETE = "DELETE"
    COPY = "COPY"


class Severity(enum.Enum):
    EMERG = "EMERG"
    CRIT = "CRIT"
    ERR = "ERR"
    WARNING = "WARNING"
    NOTICE = "NOTICE"
    INFO = "INFO"
    DEBUG = "DEBUG"
    TRACE = "TRACE"