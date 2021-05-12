"""
This file process below txt and extracts results.
"""

cmd = """
        SELECT
        T1.uckey,
        T1.price_cat,
        T1.si,
        T1.ts,
        T2.ratio
        FROM dlpm_03182021_tmp_ts AS T1 INNER JOIN dlpm_03182021_tmp_distribution AS T2
        ON T1.uckey=T2.uckey AND T1.price_cat=T2.price_cat
        WHERE T1.si='15e9ddce941b11e5bdec00163e291137' AND T1.price_cat='1'

Actual DENSE --> [34, 5, 0, 12257, 11238]
Predicted DENSE --> [24080, 21084, 19717, 18011, 19768]
Actual NON-DENSE --> [24, 4, 0, 9740, 8856]
Predicted NON-DENSE --> [8854, 9607, 10067, 11474, 9187]

"""


def c_error(x, y):
    x = x * 1.0
    if x != 0:
        e = abs(x - y) / x
    else:
        e = -1
    e = round(e, 3)
    return e


def process(line):
    a = line.find('[')
    b = line.find(']')
    l = line[a:b+1]
    l = eval(l)
    return l


def error_m(a, p):
    result = []
    for i in range(len(a)):
        x = a[i]
        y = p[i]
        e = c_error(x,y)
        result.append(e)
    x = sum(a)
    y = sum(p)
    e = c_error(x,y)
    return (e, result)


lines = cmd.split('\n')
for line in lines:
    if 'T1.si=' in line:
        line = line.replace('WHERE T1.si=', '')
        line = line.replace("AND T1.price_cat='1'", '')
        si = line.strip()
    if 'Actual DENSE' in line:
        a_d = process(line)
    if 'Predicted DENSE' in line:
        p_d = process(line)
    if 'Actual NON-DENSE' in line:
        a_nd = process(line)
    if 'Predicted NON-DENSE' in line:
        p_nd = process(line)

print(si)
print(error_m(a_d, p_d))
print(error_m(a_nd, p_nd))

x = sum(a_d)+sum(a_nd)
y = sum(p_d)+sum(p_nd)
e = c_error(x,y)
print(e)

