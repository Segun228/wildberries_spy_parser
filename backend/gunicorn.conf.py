import multiprocessing


bind = "0.0.0.0:8000"
workers = 4
worker_class = "sync"

accesslog = "-"
errorlog = "-"
loglevel = "debug"
capture_output = True