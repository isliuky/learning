import os

def listFiles(dirPath):
    '''遍历指定文件夹下打印所有的文件
    :param    dirPath: 指定遍历的文件夹路径p
    :return:  一个列表，包含指定文件夹下所有的文件绝对路径
    '''
    # 准备一个空列表，用来存储遍历数据
    fileList = []
    ''' os.walk(dirPath) ：走查指定的文件夹路径
            root  ：代表目录的路径
            dirs  ：一个list，包含了dirpath下所有子目录文件夹的名字
            files ：一个list，包含了所有非目录文件的名字
    '''
    for root, dirs, files in os.walk(dirPath):
        # 循环遍历列表：files【所有文件】，仅得到不包含路径的文件名

        for fileObj in files:
            # 空列表写入遍历的文件名称，兵勇目录路径拼接文件名称
            fileList.append(os.path.join(root, fileObj))

    # 打印一下列表存储内容：指定文件夹下所有的文件名
    # print(fileList)
    return fileList


def replace_str(fileDir, old_str, new_str):
    '''
    :param fileDir: 需要替换字符串的文件夹的绝对路径
    :param old_str: 替换前的字符串
    :param new_str: 替换后的字符串
    :return: 替换了字符串后重新写入内容的文件
    '''
    # 调用上面的 listFiles() 方法,获取到所有文件名称的列表
    fileList = listFiles(fileDir)
    # 循环遍历列表内容
    for file_name in fileList:
        print(f'file_name:{file_name}')
        # os.path.splitext() 方法，分割文件路径和后缀，返回列表类型的数据
        # 示例 ："D:\\test\\text.csv" 分割为：[ "D:\\test\\text" , ".csv" ]
        res = os.path.splitext(file_name)
        # 取出返回列表，下标为 1 的元素，即为文件名后缀字段
        file_extension = res[1]
        print(f'file_extension:{file_extension}')
        # 例如：我需要替换后缀为 .html 的文件内容
        if file_extension == ".html" or file_extension == ".json" or file_extension == ".txt":
            # 以 utf-8 的编码格式打开并读写文件
            f = open(file_name, 'r+', encoding='utf-8')
            # readlines() 一次性读取所有行文件,可以遍历结果对每一行数据进行处理
            all_the_lines = f.readlines()
            # seek()方法，操作文件游标移动操作，0代表游标移动到文件开头
            f.seek(0)
            # truncate()方法，从光标所在位置进行截断【readlines() 一次性读取所有行文件，所以截取的就是全文】
            f.truncate()
            # 循环遍历文件内容的的每一行字段
            for line in all_the_lines:
                # 替换【待替换字段】后重新写入文件内容
                f.write(line.replace(old_str, new_str))
            # 关闭文件
            f.close()
        else:
            print("不符合标准")

# if __name__ == '__main__':
    # 文件夹路径为变量时，可以使用 os.getcwd() 动态获取
# fileDir = os.getcwd()

# 手动指定需要替换字符换的文件夹路径
fileDir = r'C:\Bayer\BayerGit\learning\PythonSpease\Bayer\VR\scripts\config'

# replace_str(文件夹路径，替换前的字符串，替换后的字符串)
replace_str(fileDir, '268754486553', '828533277754')
replace_str(fileDir, '850255527329', '828533277754')

replace_str(fileDir, 'ph-cdp-prod-cn-north-1', 'ph-cdp-nprod-qa-cn-north-1')
replace_str(fileDir, 'ph-cdp-nprod-dev-cn-north-1', 'ph-cdp-nprod-qa-cn-north-1')

replace_str(fileDir, 'cn-north-1-prod', 'cn-north-1-qa')
replace_str(fileDir, 'cn-north-1-dev', 'cn-north-1-qa')

replace_str(fileDir, 'ph-cdp-sftp-outbound-prod', 'ph-cdp-sftp-outbound-qa')
replace_str(fileDir, 'ph-cdp-sftp-outbound-dev', 'ph-cdp-sftp-outbound-qa')

replace_str(fileDir, 'ph-cdp-sftp-inbound-prod', 'ph-cdp-sftp-inbound-qa')
replace_str(fileDir, 'ph-cdp-sftp-inbound-dev', 'ph-cdp-sftp-inbound-qa')

replace_str(fileDir, 'From_prod', 'From_qa')
replace_str(fileDir, 'From_dev', 'From_qa')

replace_str(fileDir, 'cn_cdp_prod', 'cn_cdp_qa')
replace_str(fileDir, 'cn_cdp_dev', 'cn_cdp_qa')

# replace_str(fileDir, '"prod"', '"qa"')
# replace_str(fileDir, '"dev"', '"qa"')

replace_str(fileDir, '-dev-cn-north-1', '-qa-cn-north-1')
replace_str(fileDir, '-prod-cn-north-1', '-qa-cn-north-1')

replace_str(fileDir, 'phcdp/mssql/fusion-p-1', 'phcdp/mssql/fusion-q')
replace_str(fileDir, 'phcdp/mssql/mao-p','phcdp/mssql/mao-q')