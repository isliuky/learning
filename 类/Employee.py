class Employee:
    def __init__(self,name, id):
        self.name = name
        self.id = id

    def print_info(self):
        print(f'我的名字是{self.name},我的工号是{self.id}')

    def calucate_monthly_pay(self):
        print("都有这个方法")




class FullTimeEmployee(Employee):
    def __init__(self, name, id,month_salary):
        super().__init__(name,id)
        self.month_salary = month_salary





class ParrTimeEmployee(Employee):
    def __init__(self, name, id,daily_salary,work_days):
        super().__init__(name,id)
        self.month_salary = daily_salary
        self.month_salary = work_days

full = FullTimeEmployee("lisi",12,1000)
part = ParrTimeEmployee("zhangsan",21,100,10)
full.print_info()
part.print_info()
print(full.calucate_monthly_pay())
# print(full.month_salary)