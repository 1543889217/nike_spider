from hdfs import InsecureClient
import json
import traceback


class HdfsClient(InsecureClient):
    """
    继承hdfs的InsecureClient类，新添加自己的方法
    """
    def __init__(self, url, user, **kwargs):
        super().__init__(url, user, **kwargs)
        self.is_first = True
        self.file_path_list = []  # 写入的文件列表
        self.file_path_list_item = {}  # 文件对应的是否第一次写入关系

    def new_write(self, path, records, encoding='utf-8'):
        """
        新的写入方法，判断文件是否存在，是新建还是追加数据
        :param path:
        :param records:
        :param encoding:
        :return:
        """
        self.file_path_list = []  # 写入的文件列表
        self.file_path_list_item = {}  # 文件对应的是否第一次写入关系
        file_name = path.split('/')[-1]
        if file_name not in self.file_path_list:
            self.file_path_list_item[file_name] = True   # 一个脚本要写入多个文件的时候，要分别做第一次写入判断
            self.file_path_list.append(file_name)

        if self.file_path_list_item[file_name]:
            folder_path = '/'.join(path.split('/')[:-1])
            file_list = self.list(folder_path)
            if file_name in file_list:  # 如果文件存在，则追加数据
                self.write(path, data=records, encoding=encoding, append=True)
            else:   # 不存在，则新建文件写入
                self.write(path, data=records, encoding=encoding)
                self.file_path_list_item[path] = False
        else:
            self.write(path, data=records, encoding=encoding, append=True)



if __name__=="__main__":
    pass