class Student:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.gender = {"语文": 0, "数学": 0, "英语": 0}

    def chage_gender(self, kemu, score):
        if kemu in self.gender:
            self.gender[kemu] = score


student1 = Student("张三", "001")
student2 = Student("李四", "002")
student2.chage_gender("语文",80)
print(student1.name,student1.gender)
print(student2.name,student2.gender)
