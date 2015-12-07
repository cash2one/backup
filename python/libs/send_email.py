#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author : luoruiyang
# date   : 2014-07-09
# func   : send mail with img, attachment, html
import os
import sys
try:
    import smtplib
    from email.MIMEText import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.image import MIMEImage
except Exception as info:
    print info
    sys.exit()
import tools
file_logger = tools.logger("./logs/send.email")

class SendMailToLists(object):
    MESSAGE_ENCODE = "utf-8"
    FROM_ADDR = "postmaster@baijiahulian.com"
    MAIL_SERVER = "smtpproxy.baijiahulian.com"  # 设置服务器
    MAIL_PORT = 25
    MAIL_USER = "postmaster"
    MAIL_PASS = "bjhl123456"  # 口令

    MAIL_SERVER_BAK = "smtp.126.com"
    MAIL_USER_BAK = "icygaara"
    MAIL_FROM_BAK = "icygaara@126.com"
    MAIL_PASS_BAK = "icygaaratbs"

    def __init__(self, data = {}, logger = None):
        self.data = data
        self.MSG = None
        self.logger = logger
        if not self.logger:
            self.logger = file_logger

    def set_data(self, data):
        self.data = data
        return self

    def make_msg(self):
        if "To" not in self.data or "Body" not in self.data:
            self.logger.fatal("To and Body must be set")
            return self

        try:
            self.MSG = MIMEMultipart()
            # self.MSG["From"] = self.data.get("From", self.FROM_ADDR)
            self.MSG["To"] = ";".join(self.data["To"])
            self.MSG["Subject"] = self.data.get("Subject", "Default Subject")
            self.MSG["Cc"] = ";".join(self.data.get("Cc", []))
            self.MSG["Bcc"] = ";".join(self.data.get("Bcc", []))
            self.MSG.attach(MIMEText(self.data["Body"].encode(self.MESSAGE_ENCODE), self.data.get("Type", "html")))
            if "img" in self.data:
                self.add_img()
            if "attach" in self.data:
                self.add_attach()

        except Exception as info:
            # self.sms(str(info.message).replace(" ", "_"))
            self.MSG = None
        return self

    def add_img(self):
        for img in self.data["img"]:
            img_fp = open(img[0], "rb")
            msg_img = MIMEImage(img_fp.read())
            img_fp.close()

            msg_img.add_header("Content-ID", img[1])
            self.MSG.attach(msg_img)

    def add_attach(self, encode_base = "base64", text_encode = "utf-8"):
        for att in self.data["attach"]:
            msg_att = MIMEText(open(att[0], "rb").read(), encode_base, text_encode)
            msg_att["Content-Type"] = "application/octet-stream"
            msg_att["Content-Disposition"] = 'attachment;filename="' + str(att[1]) + '"'
            self.MSG.attach(msg_att)

    def send(self):
        if not self.MSG:
            self.logger.fatal("run make_msg before send")
            sys.exit()
        try:
            smtp = smtplib.SMTP()
            try:

                code, server_msg = smtp.connect(self.MAIL_SERVER, port = self.MAIL_PORT)
                self.MSG["From"] = self.data.get("From", self.FROM_ADDR)
                # smtp.login(self.MAIL_USER, self.MAIL_PASS)  #登陆服务器
            except Exception as info:
                self.logger.warn("User Bak connect.")
                code, server_msg = smtp.connect(self.MAIL_SERVER_BAK)
                smtp.login(self.MAIL_USER_BAK, self.MAIL_PASS_BAK)
                self.MSG["From"] = self.MAIL_FROM_BAK

            self.data["To"].extend(self.data.get("Cc", []))
            self.data["To"].extend(self.data.get("Bcc", []))
            self.logger.info("send to " + str(self.data["To"]))
            smtp.sendmail(self.MSG["From"], self.data["To"], self.MSG.as_string())
            smtp.quit()
            self.logger.info("send success")
        except Exception as info:
            self.logger.fatal("send_email->send [%s]" % info)
            # self.sms(str(info.message).replace(" ", "_"))

    def sms(self, msg, phone = "15801689885"):
        print msg
        return
        cmd = "gsmsend -s emp01.baidu.com:15003 -s emp02.baidu.com:15003 %s@%s" % (phone, msg)
        os.system(cmd)

if __name__ == "__main__":
    data = {
            "To" : ["luoruiyang@baijiahulian.com"],\
            "Subject" : "test cc",\
            "Body" : "test, hello world!!!",
            "attach": [("sheet_1.xls", "用户数据.xls")]}
    # Usage
    SMTL = SendMailToLists(data)
    SMTL.make_msg()\
        .send()
