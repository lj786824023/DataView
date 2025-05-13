import configparser


class ConfigFile:
    """
    使用configparser模块读写INI格式配置文件的类
    """

    def __init__(self, file_path, encoding):
        """
        初始化方法，传入配置文件路径
        :param file_path: 配置文件路径
        :param encoding: 配置编码格式
        """
        self.file_path = file_path
        self.encoding = encoding

    def read(self):
        """
        读取配置文件内容
        :return: 返回一个字典，键为节名，值为字典，键为选项名，值为选项值
        """
        config = configparser.ConfigParser()
        config.read(self.file_path, encoding=self.encoding)
        return {section: dict(config.items(section)) for section in config.sections()}

    def write(self, data_dict):
        """
        写入配置文件内容
        :param data_dict: 字典类型数据，包含配置文件内容
        """
        config = configparser.ConfigParser()
        for section, options in data_dict.items():
            config.add_section(section)
            for option, value in options.items():
                config.set(section, option, value)

        with open(self.file_path, 'w', encoding=self.encoding) as f:
            config.write(f)

    def update(self, section, data_dict, new_section=None):
        """
        更新配置文件指定节的内容
        :param section: 节名
        :param data_dict: 字典类型数据，包含要更新的选项及其值
        :param new_section: 新的节名，如果为None，则不修改节名
        """
        config = configparser.ConfigParser()
        config.read(self.file_path, encoding=self.encoding)
        if not config.has_section(section):
            config.add_section(section)

        for option, value in data_dict.items():
            config.set(section, option, value)

        if new_section is not None and section != new_section:
            # 修改节名
            config.add_section(new_section)
            for option, value in data_dict.items():
                config.set(new_section, option, value)
            config.remove_section(section)

        with open(self.file_path, 'w', encoding=self.encoding) as f:
            config.write(f)

    def delete(self, section, option=None):
        """
        删除配置文件指定节或选项
        :param section: 节名
        :param option: 选项名，如果为None，则删除整个节
        """
        config = configparser.ConfigParser()
        config.read(self.file_path, encoding=self.encoding)
        if config.has_section(section):
            if option is None:
                config.remove_section(section)
            else:
                config.remove_option(section, option)

            with open(self.file_path, 'w', encoding=self.encoding) as f:
                config.write(f)
        else:
            print("节点不存在！")


if __name__ == "__main__":
    # 实例化一个ConfigFile对象
    config = ConfigFile('example.ini', 'UTF-8')

    # 写入配置文件内容
    new_data_dict = {
        'web_server': {
            'host': '127.0.0.1',
            'port': '8080'
        },
        'user_information': {
            'account': 'admin',
            'password': '123'
        }
    }
    config.write(new_data_dict)

    # 读取配置文件内容
    data_dict = config.read()
    print(data_dict)
    print(data_dict['web_server']['port'])

    # 更新配置文件内容
    update_data_dict = {
        'host': 'localhost',
        'port': '8888'
    }
    config.update('web_server', update_data_dict, new_section='new_web_server')

    # 删除配置文件内容
    config.delete('new_web_server', 'host')
