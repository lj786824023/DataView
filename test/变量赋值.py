# 天平式写法
a = 1
b = 2
c = a if a > b else b
# print(c)

# 条件赋值，做对象操作而不确定变量名是否有效
# 例如 None [] () '' 0 等bool值都是False,类通过__bool__可以使类支持bool运算，从而支持if while and等运算
a = []
b = a.pop() if a else 0
print(b)
