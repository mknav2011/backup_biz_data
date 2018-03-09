
#Method 1:
def list_of_integers():
    '''
    Returns a list of integers from 17 to 100 that are evenly divisible by 11.
    '''
    integer_list = []
    for i in range(17,100):
        if i % 11 == 0:
            integer_list.append(i)
    print integer_list


#Method 2:
def list_of_integers_2():
    '''
    Returns a list of integers from 17 to 100 that are evenly divisible by 11.
    '''
    x = [i for i in range(17, 100) if i % 11 == 0]
    print x

def dict_mapping():
    '''
    Returns a dictionary mapping integers to their 2.75th root for all integers
    from 2 up to 100 (including the 2.75th root of 100).
    '''
    my_dict_mapping = {}
    import math
    for i in range(2,101):
        my_dict_mapping[i] = pow(i, 1/2.75)
        #print math.pow(2, 1/2.75)
    print my_dict_mapping

#print dict_mapping()


def find_ips(inp):
    import re
    '''
    Returns a list of ip addresses of the form 'x.x.x.x' that are in the input
    string and are separated by at least some whitespace.
    >>> find_ips('this has one ip address 127.0.0.1')
    ['127.0.0.1']
    >>> find_ips('this has zero ip addresses 1.2.3.4.5')
    []
    '''
    #print inp.split(" ")
    ip = []
    for i in inp.split(" "):
        if i.count('.')==3:
            ip = re.findall( r'[0-9]+(?:\.[0-9]+){3}', i )
    return ip

#print find_ips('this has one ip 999 address 127.0.0.1')
#print find_ips('this has zero ip addresses 1.2.3.4.5')


def generate_cubes_until(modulus):
    import math
    '''
    Generates the cubes of integers greater than 0 until the next is 0 modulo
    the provided modulus.
    >>> list(generate_cubes_until(25))
    [1, 8, 27, 64]
    '''
    out_put = []
    for i in range(1, modulus ):
        if (i ** 3) % modulus == 0:
            break
        out_put.append( i ** 3)
        #print (i ** 3) % modulus
    return out_put
#print generate_cubes_until(25)

def generate_cubes_until_amit(x):
    cubes = []
    for i in range(1,x):
        if (i**3)%x==0:
            break
        cubes.append(i**3)
    return cubes
#print generate_cubes_until_amit(25)


def total_size(filenames):
    '''
    Given a list of filenames in Apache Common Log format, return a mapping
    of the total number of responses of different types to the total number of
    bytes returned by all responses of that type.
    >>> total_size(['/var/log/httpd.log', '/var/log/httpd.log.1'])
    {'200': 7362899, '404': 28710, ...}

    The format can be found: https://en.wikipedia.org/wiki/Common_Log_Format
    And an example line of common log format is:
    127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
    '''
    size_source = []
    log_sizes_dict = {}
    for filename in filenames:
        file = open(filename, 'r')
        for line in file:
            size_source.append((line.split(" ")[-2].strip(),int(line.split(" ")[-1].strip())))

    for tup in size_source:
        if tup[0] in log_sizes_dict.keys():
            log_sizes_dict[tup[0]]=int(log_sizes_dict[tup[0]])+int(tup[1])
        else:
            log_sizes_dict[tup[0]] = tup[1]

    print log_sizes_dict

#total_size(["/var/log/httpd1.log"])


def check_type(typ):
    '''
    Write a function decorator that takes an argument and returns a decorator
    that can be used to check the type of the argument to a 1-argument function.
    Should raise a TypeError if the wrong argument type was passed.
    >>> @check_type(int)
    ... def test(arg):
    ...   print arg
    ...
    >>> test(4)
    4
    >>> test(4.5) # raises TypeError because 4.5 isn't an int type
    '''
    def type_check(typ, *args):
        print(args[0])
        if isinstance(args[0], typ):
            typ(*args, **kwargs)
        else:
            raise TypeError
    return type_check

@check_type(int)
def test(val):
    print (val)

test(4)
class myDecorator(object):

    def __init__(self, f):
        print "inside myDecorator.__init__()"
        f() # Prove that function definition has completed

    def __call__(self):
        print "inside myDecorator.__call__()"

@myDecorator
def aFunction():
    print "inside aFunction()"

print "Finished decorating aFunction()"

aFunction()

