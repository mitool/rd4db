# rd4db
从数据库中删除重复的数据

本程序从MySQL数据表中查询出appid的值，然后根据得到的appid定位到MongoDB中的相应数据集合，并判断数据是否重复，如果重复则删除。

本程序使用[github.com/webx-top/db](https://github.com/webx-top/db)来进行数据库操作。

从这个小程序中我们可以充分学习到如何用[github.com/webx-top/db](https://github.com/webx-top/db)来同时连接不同的数据库，以及一些基本的操作方式。
