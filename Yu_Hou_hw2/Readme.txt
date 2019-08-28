Python 2.7
Spark version 2.2.1

Task1
EC2:
/home/ec2-user/spark/spark-2.2.1-bin-hadoop2.7/bin/spark-submit Yu_Hou_PCY.py baskets.txt <a> <b> <N> <s> <output-dir>
/home/ec2-user/spark/spark-2.2.1-bin-hadoop2.7/bin/spark-submit Yu_Hou_PCY.py baskets.txt 11 29 150 2500 output

Local:
bin/spark-submit Yu_Hou_PCY.py baskets.txt <a> <b> <N> <s> <output-dir>
bin/spark-submit Yu_Hou_PCY.py baskets.txt 11 29 150 2500 output

Explain:
This will create a dictionary "output".
In this dictionary, you will find "candidates.txt" and "frequent.txt"
On console, you will find "False Positive Rate:".

Task2 Python Part (Version: Python 2.7):
EC2:
/home/ec2-user/spark/spark-2.2.1-bin-hadoop2.7/bin/spark-submit Yu_Hou_SON.py baskets.txt <support> output.txt
/home/ec2-user/spark/spark-2.2.1-bin-hadoop2.7/bin/spark-submit Yu_Hou_SON.py baskets.txt 0.2 output.txt

Local:
bin/spark-submit Yu_Hou_SON.py baskets.txt <support> output.txt
bin/spark-submit Yu_Hou_SON.py baskets.txt 0.2 output.txt

Explain:
This will create a file "output.txt". In this file, you could see the all frequent items.
Notes: If <support> = 0.5, it will cost 20 seconds
If <support> = 0.2, it will cost 4 minutes
If <support> = 0.1, it will cost 9 minutes.
