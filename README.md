# Extraits du projet Kanyon - (financial markets)



#-# Objectif : 

L’objectif du projet « kanyon » est de pouvoir accéder aux données d’une panoplie de marchés, partant de l’extraction au stockage et au final à la manipulation de la data.


#-# Moyens : 

En se basant sur la librairie standard de python ainsi que d’autres librairies (Selenium, XML etree..) pour l’extraction , le travail sur la data se fait principalement via deux structures :

  -> List / dict : structures standards de Python
  -> DataFrame : structure issue de la librairie Pandas.

La manipulation de la base de données se fait via SQL Alchemy qui est un ORM (object relational mapper), ce qui a permis d’automatiser les requêtes SQL et a facilité l’implémentation de la base de données ainsi que l’apport de modifications majeures sur la base de données.
