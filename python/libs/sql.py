#!/usr/bin/env python


class SQL(object):
    def __init__(self):
        self.sql = ""

    def get(self):
        return self.sql

    def get(self, Select, From, Where = False, GroupBy = False, OrderBy = False):
        sql = ["SELECT",
               self.select(Select),
               "FROM",
               From,
               self.where(Where),
               self.group_by(GroupBy),
               self.order_by(OrderBy)]
        self.sql = " ".join(sql).strip()
        return self.sql

    def select(self, sel):
        ret = []
        for s in sel:
            ret.append(self.add_quote(s))
        return ",".join(ret)

    def is_special(self, colum):
        if colum.strip() == "*":
            return True
            
        if not colum.strip().lower().startswith("date_format(") and\
           not colum.strip().lower().startswith("sum(") and\
           not colum.strip().lower().startswith("count(") and\
           not colum.strip().lower().startswith("`") and\
           not colum.find(".") != -1:
           return False
        else:
           return True

    def where(self, w):
        if not w:
            return ""
        conditions = []
        for key in w:
            value = w.get(key)
            key = self.add_quote(key)
            conditions.append(" ".join([key, value]))
        if not len(conditions):
            return ""
        return "WHERE " + " AND ".join(conditions)

    def group_by(self, group):
        if not group:
            return ""

        return "GROUP BY %s" % ",".join(map(self.add_quote, group))

    def order_by(self, order):
        if not order:
            return ""

        return "ORDER BY %s" % ",".join(map(self.add_quote, order))

    def add_quote(self, column):
        if self.is_special(column):
            return "%s" % column
        return "`%s`" % column

if __name__ == "__main__":
    sql = SQL()
    print sql.get(["id", "value"],
            "cdb.user",
            {"id": "= 12", "value": "< 100"},
            GroupBy = ["id"],
            OrderBy = ["value"]
             )
