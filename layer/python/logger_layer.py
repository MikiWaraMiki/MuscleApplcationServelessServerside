from logging import getLogger, StreamHandler, Formatter, basicConfig,WARN,DEBUG

class ApplicationLogger():
    def __init__(self,name=__name__, env_info="", log_format="%(asctime)s %(levelname)s %(name)s :%(message)s"):
        # loggerオブジェクト生成
        self.name = name
        self.logger  = getLogger(name)
        self.handler = StreamHandler()
        self.handler.setFormatter(Formatter(log_format))
        self.logger.addHandler(self.handler)
        self.logger.propagate = False
        self.setLogLevel(env_info)
    def setLogLevel(self,env_info):
        #環境に合わせてログの出力レベルを変更する
        if env_info == "prod":
            self.logger.setLevel(WARN)
        else:
            self.logger.setLevel(DEBUG)
    
    def debug(self,message):
        self.logger.debug(message)

    def info(self,message):
        self.logger.info(message)

    def warn(self,message):        
        self.logger.warn(message)

    def error(self,message):
        self.logger.error(message)