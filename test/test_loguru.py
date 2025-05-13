

from loguru import logger

# 新增long文件，可以多个
# logger.add("C:\\Users\\lojn\\PycharmProjects\\DataView\\_internal\\aaa_log\\loguru.log")
# log文件按大小分类
logger.add("C:\\Users\\lojn\\PycharmProjects\\DataView\\_internal\\aaa_log\\loguru.log")
logger.debug("this is debug")
logger.info("this is info")
logger.warning("this is warning")
logger.error("this is error")
