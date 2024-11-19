class CuteCat:
    def __init__(self, cat_name, cat_age, cat_color):
        self.cat_age = cat_age
        self.cat_name = cat_name
        self.cat_color = cat_color


cat = CuteCat("lambda", 18, "orange")
print(f"猫猫的名字是：{cat.cat_name}，猫猫的年龄是{cat.cat_age}，猫猫的颜色是{cat.cat_color}")