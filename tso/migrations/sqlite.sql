CREATE TABLE TSO_TIMESTAMP (
    ID INTEGER PRIMARY KEY AUTOINCREMENT, -- 主键
    TSO_PATH TEXT NOT NULL, -- TSO 的标识
    TSO_TS TEXT NOT NULL, -- TSO 时间戳，全局唯一且自增，无符号 64 位数字
    LAST_UPDATED TIMESTAMP NOT NULL DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'now')), -- 最近更新时间
    UPDATE_NODE TEXT NOT NULL, -- 最近更新的节点信息
    UNIQUE (TSO_PATH)
);