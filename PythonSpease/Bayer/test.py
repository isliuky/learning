# ['C:\\Users\\EJOPV\\AppData\\Local\\Programs\\Python\\Python36', 'C:\\Users\\EJOPV\\AppData\\Local\\Programs\\Python\\Python36\\lib\\site-packages']
import site; print(site.getsitepackages())
# ['C:\\Users\\EJOPV\\AppData\\Local\\Programs\\Python\\Python312', 'C:\\Users\\EJOPV\\AppData\\Local\\Programs\\Python\\Python312\\Lib\\site-packages']
import os
print("PYTHONPATH:", os.environ.get('PYTHONPATH'))
print("PATH:", os.environ.get('PATH'))