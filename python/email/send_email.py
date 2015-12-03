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


class SendMailToLists(object):
    MAIL_SERVER = "email.baijiahulian.com"
    MESSAGE_ENCODE = "utf-8"
    FROM_ADDR = "auto@baijiahulian.com"

    def __init__(self, data = {}):
        self.data = data
        self.MSG = None

    def make_msg(self):
        if ("To" not in self.data) or ("Body" not in self.data):
            print "To and Body must be set"
            return self

        try:
            self.MSG = MIMEMultipart()
            self.MSG["From"] = self.data.get("From", "yf-imns-ses144.yf01.baidu.com")
            self.MSG["To"] = ";".join(self.data["To"])
            self.MSG["Subject"] = self.data.get("Subject", "default subject")
            self.MSG["Cc"] = ";".join(self.data.get("Cc", []))
            self.MSG["Bcc"] = ";".join(self.data.get("Bcc", []))
            self.MSG.attach(MIMEText(self.data["Body"].encode(self.MESSAGE_ENCODE), self.data.get("Type", "html")))
            if "img" in self.data:
                self.add_img()
            if "attach" in self.data:
                self.add_attach()

        except Exception as info:
            self.sms(str(info.message).replace(" ", "_"))
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
            print "run make_msg before send"
            sys.exit()
        try:
            smtp = smtplib.SMTP()
            smtp.connect(self.MAIL_SERVER)
            self.data["To"].extend(self.data.get("Cc", []))
            self.data["To"].extend(self.data.get("Bcc", []))
            print "send to", self.data["To"]
            smtp.sendmail(self.FROM_ADDR, self.data["To"], self.MSG.as_string())
            smtp.quit()
            print "send success"
        except Exception as info:
            self.sms(str(info.message).replace(" ", "_"))

    def sms(self, msg, phone = "15801689885"):
        print msg
        return
        cmd = "gsmsend -s emp01.baidu.com:15003 -s emp02.baidu.com:15003 %s@%s" % (phone, msg)
        os.system(cmd)

if __name__ == "__main__":
    data = {"Cc" : ["luoruiyang@baijiahulian.com"],\
            "To" : ["jiangzhen01@baidu.com"],\
            "Subject" : "test cc",\
            "Body" : "test, hello world!!!"}
    # Usage
    SMTL = SendMailToLists(data)
    SMTL.make_msg()\
        .send()
