from os import environ

dbhost = 'localhost' if 'DYNAUTO_HOST' not in environ else environ['DYNAUTO_HOST']
dbport = 80 if 'DYNAUTO_PORT' not in environ else int(environ['DYNAUTO_PORT'])
dbusr = 'dynauto' if 'DYNAUTO_USER' not in environ else environ['DYNAUTO_USER']
dbpwd = 'changeme' if 'DYNAUTO_PASS' not in environ else environ['DYNAUTO_PASS']
dbssl = False
