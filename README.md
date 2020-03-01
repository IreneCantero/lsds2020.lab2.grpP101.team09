# LSDS-Lab2

<h3>**Introduction**</h3>

<p align = justify>This document contains all the benchmarks and results requested in this lab. However, we would like to use this README to clarify some nuances we had in the code.
First of all, perhaps the implementation is not the most optimal one, because the code is the result of thinking little parts and then coding it; i.e. we did
many little steps to reach the solution. The second thing is regarding the input, because we followed the order of Lab 1 instead of the proposed one by David Solans
in the forum. The input order of each exercise is as follows:
</p>
<ul>
	 <li>For exercise 1: language, output directory, inputs</li>
	 <li>For exercise 2: language, output directory, bucket, inputs.</li>
		<ul>
		 <li>NOTE: we were using a previous version of the program. That is why there is an unused argument "bucket". In the final version of the program the inputs are
		like in exercise 1. We put this clarification, because in the the arguments provided in the steps of the cluster follows this format. Sorry for the inconvenience.</li>
		</ul>
	 <li>For exercise 3: language, inputs</li>
	 <li>For exercise 4: inputs</li>
</ul>
<p align = justify>The last thing we would like to point out is that we created a *MoreExtendedSimplifiedTweet* class to get some information of the retweeted tweets from the retweets. However, at the end we realized that perhaps it is rendundant this class, because we could get the same information in *ExtendedSimplifiedTweet*.</p>

<p>Our bucket link is: https://s3.console.aws.amazon.com/s3/buckets/edu.upf.ldsd2020.lab2.grp101.team09/?region=us-east-1&tab=overview</p>

------------------------------------------------------------------------------------------------

**Exercise 2 BENCHMARKS WITH DIFFERENT NUMBER OF SLAVES:**

*es:*

*WITH 2 SLAVES:*
``` 
Language: es. Output file: s3://edu.upf.ldsd2020.lab2.grp101.team09/output/run3. Destination bucket: eurovisiondata
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json: 35.012318 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json: 46.80524 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json: 26.765764 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json: 40.54886 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json: 28.787739 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json: 23.675217 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json: 17.510193 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json: 107.02887 s
``` 
**Simplified tweets:509435**
**Total time: 326.1342 s**

*WITH 8 SLAVES:*

``` 
Language: es. Output file: s3://edu.upf.ldsd2020.lab2.grp101.team09/output/run4. Destination bucket: eurovisiondata
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json: 27.060328 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json: 15.430321 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json: 6.206684 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json: 7.008059 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json: 5.9260225 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json: 5.7052336 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json: 3.3643157 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json: 21.795488 s
``` 

**Simplified tweets:509435**
**Total time: 92.49645 s**


*hu:*

*WITH 2 SLAVES:*
``` 
Language: hu. Output file: s3://edu.upf.ldsd2020.lab2.grp101.team09/output/run2. Destination bucket: eurovisiondata
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json: 34.572437 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json: 45.83916 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json: 25.860785 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json: 38.556786 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json: 26.541222 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json: 21.525988 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json: 15.588707 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json: 101.759575 s
``` 
**Simplified tweets:1057**
**Total time: 310.24466 s**

*WITH 8 SLAVES:*
``` 
Language: hu. Output file: s3://edu.upf.ldsd2020.lab2.grp101.team09/output/run5. Destination bucket: eurovisiondata
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json: 28.659586 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json: 14.449773 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json: 6.132486 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json: 6.97662 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json: 5.7863092 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json: 5.7047806 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json: 3.2937684 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json: 16.356796 s
``` 
**Simplified tweets:1057**
**Total time: 87.360115 s**


*pt:*

*WITH 2 SLAVES:*
``` 
Language: pt. Output file: s3://edu.upf.ldsd2020.lab2.grp101.team09/output/run1. Destination bucket: eurovisiondata
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json: 35.85395 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json: 46.43433 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json: 27.040339 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json: 41.139324 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json: 27.83488 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json: 22.77989 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json: 16.432709 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json: 101.85284 s
``` 
**Simplified tweets:37623**
**Total time: 319.36826 s**

*WITH 8 SLAVES:*
``` 
Language: pt. Output file: s3://edu.upf.ldsd2020.lab2.grp101.team09/output/run6. Destination bucket: eurovisiondata
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision3.json: 29.098764 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision4.json: 14.069407 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision5.json: 5.799392 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision6.json: 6.9521055 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision7.json: 6.0536623 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision8.json: 6.1692133 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision9.json: 3.5396962 s
Processing: s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json
Partial time for s3://edu.upf.ldsd2020.lab2.grp101.team09/input/Eurovision10.json: 21.752651 s
``` 
**Simplified tweets:37623**
**Total time: 93.43489 s**

------------------------------------------------------------------------------------------------


**Exercise 3 TOP BIGRAMS FOR SOME GIVEN LANGAUGES:**

*en:* 
``` 
[((of,the),5773), ((in,the),5162), ((for,the),4279), ((the,eurovision),3850), ((this,is),3203), ((is,the),2826), ((eurovision,song),2645), ((to,be),2607), (("this,is),2539), ((on,the),2477)]
``` 
*es:*
``` 
[((de,la),3407), ((que,no),2290), ((en,el),2129), ((de,#eurovision),1928), ((#eurovision,#finaleurovision"),1927), ((la,canción),1909), ((lo,que),1810), ((en,la),1782), ((a,la),1759), ((que,me),1479)]
``` 
*pt:*
``` 
[((é,que),225), ((que,a),221), ((a,música),184), ((o,ano),164), ((para,o),149), ((o,que),144), ((de,israel),139), ((com,a),137), ((#eurovision,#allaboard"),132), ((música,da),123)]
``` 
*hu:*
``` 
[((viszlát,nyár),41), ((-,viszlát),33), ((-,hungary),31), ((hungary,-),31), ((nyár,-),30), ((-,eurovision),28), ((aws,-),25), ((ez,a),23), (("#eurovisionhun,#eurovision),23), ((-,live),22)]
``` 
------------------------------------------------------------------------------------------------


**Exercise 4 TOP 10 RETWEETED TWEETS FROM THE TOP 10 MOST RETWEETED USERS:**
``` 
(40059553,465209499281014784,21913,eurovision is basically an episode of glee where europe decides their political disagreements by having a karaoke contest), (62513246,863484966021320710,18669,Portugal é o meu favorito até agora #Eurovision), 
(437025093,995406052190445568,16882,Oye Israel, que esto lo hice yo antes que tu... #Eurovision #FinalEurovision https://t.co/bBnDjB9s4z),
(38381308,995388604045316097,12957,Puigdemont ha logrado aparecerse durante unos segundos en el vestido de la participante de Estonia y ha proclamado… https://t.co/wbZc5R9Z4T), (24679473,995397243426541568,11567,See what he did there? #Eurovision #CzechRepublic  #CZE https://t.co/DwdfXmTqXg), 
(1501434991,995394150978727936,6291,Muy bien Alemania secuestrando a Ed Sheeran y poniéndole una peluca. #Eurovision), 
(39538010,995383476181401601,4597,Already hate it 0/10 #Eurovision #esp), 
(1264023596,995260351385161728,4437,Is there anything better than watching Eurovision? Yes, watching EUROVISION IN SIGN LANGUAGE 😂 https://t.co/FE3G51s9xe),
(15584187,995433967351283712,4039,The Winner of the 2018 #Eurovision Song Contest is ISRAEL! #ESC2018 #AllAboard https://t.co/Myre7yh3YV), 
(3260160764,995397610927263745,3146,Cuando tienes que cantar en Eurovisión a las 9 pero estar en El Muro a las 11 #Eurovision https://t.co/w7hQdCBxx7)]
``` 

