# NCI_MAPPING
2022-10-28

改用spark来实现数据映射。

本地的机子不太行，内存不够，跑不了LUBM100M+的

等服务器整理好了，在服务器上试试。

nci_mapping_2.11-0.1.jar + 数据集文件 + 输出文件夹


[2022-12-19]

【重新修改NCI_MAPPING模块】

想法：减少使用java对象，直接利用spark dataframe + zipWithIndex + join。[还比之前的方法快好多]
