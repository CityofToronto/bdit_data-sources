Modelling Traffic Volume Data
================
Andrew Louis
January 24, 2018

This readme will provide information regarding the Miovision traffic volume model. It will go over the objectives, background info and other different steps involved in approaching this model.

Objective of the Model
----------------------

The objective of this model is to predict baseline average traffic volume values for historical data. Several different variables are incorporated into this model.

-   Response Variable:

    -   Traffic Volume

-   Explanatory Variables:

    -   Intersection Leg (N, S, E, W) - a string corresponding to which part of the intersection the volume is coming from
    -   Direction (NB, SB, EB, WB) - a string corresponding to the direction the traffic is headed
    -   Day (1, 2, 3, 4, 5) - an integer corresponding to the weekday the volume was counted; i.e. 1 = Monday, 5= Friday, etc
    -   Dt (Datetime Value) - datetime variable corresponding to the date the volume was counted

<p>
The model should consider these different inputs and their potential interactions, producing an appropriate baseline value. There are five primary steps taken in modelling this problem.

1.  Running an SQL query to extract necessary data

2.  Connecting R to the SQL query

3.  Simple Linear Regression

4.  Multilevel Regression

5.  Multiplicative Regression
    <p>

### Running an SQL query to extract necessary data

<p>
Data is grabbed via an `.SQL` file containing a query. The file is `final.sql`. The query essentially joins multiple subqueries on different attributes. The query was made with 6 very simple parts:

-   `dt` and `intersection_uid` are queried from the `miovision.volumes_15min` table
-   `intersection_uid`, `leg`, `dir` are queried and joined on `intersection_uid` from the previous query
-   A time bin attribute is generated for each `intersection_uid`, `dt`, `leg`, `dir` combination
-   Data is added to the above query by joining data on the `time`, `intersection_uid`, `dt`, `leg`, `dir` combinations- resulting in a sparse data query
-   Date is added in missing time bin according to averages of historic data
-   Data is aggregated (summed) over 24 hours. Intersection 30 is excluded due to the incorrect directions on November 8th.

Refer to the `final.sql` document for further details.
<p>
### Connecting to R

We import relevant modules and extract the data from `final.sql` using the `RPostgreSQL` package. The data is read from `readlines()`, and the data is assigned to the variable `data`.

A new column, `day`, is added, containing the integer value corresponding to day of week. We factor the day and intersection as they are integer values.

``` r
# Import Relevant Packages
library(RPostgreSQL)
```

    ## Loading required package: DBI

``` r
library(arm)
```

    ## Loading required package: MASS

    ## Loading required package: Matrix

    ## Loading required package: lme4

    ## 
    ## arm (Version 1.9-3, built: 2016-11-21)

    ## Working directory is C:/Users/alouis2/Documents

``` r
library(lme4)
library(gnm)

drv <- dbDriver("PostgreSQL")
source("C:\\Users\\alouis2\\Documents\\R\\connect\\connect.R")
filepath = "C:\\Users\\alouis2\\Documents\\final.sql"
strSQL = readLines(filepath, encoding = "UTF-8")
data <- dbGetQuery(con, strSQL)
data$day <- as.POSIXlt(data$dt)$wday
data$day <- factor(data$day)
data$intersection_uid <- factor(data$intersection_uid)
```

### Modelling

We start with simple linear regression, then multilevel modelling, and we conclude with a multiplicative model.

#### **Simple Linear Regression**

Simple model with a single intercept, no other explanatory variables.

*V**o**l* = *β*<sub>0</sub> + *e**r**r**o**r*

*e**r**r**o**r* ∼ *N*(0, *σ*<sup>2</sup>)

``` r
grandmean = lm(data$totaladjusted_vol ~ 1)
summary(grandmean)
```

    ## 
    ## Call:
    ## lm(formula = data$totaladjusted_vol ~ 1)
    ## 
    ## Residuals:
    ##     Min      1Q  Median      3Q     Max 
    ## -7625.1 -2520.8  -637.6  2298.2 11928.9 
    ## 
    ## Coefficients:
    ##             Estimate Std. Error t value Pr(>|t|)    
    ## (Intercept)  9176.08      93.47   98.17   <2e-16 ***
    ## ---
    ## Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
    ## 
    ## Residual standard error: 3743 on 1603 degrees of freedom

<p>
We perform a simple linear regression, incorporating the other explanatory variables. We exclude `dt`, as incorporating datetime values into this simple linear regression is quite complex.
<p>
*V**o**l* = *i**n**t**e**r**s**e**c**t**i**o**n* + *l**e**g* + *d**i**r**e**c**t**i**o**n* + *d**a**y* + *e**r**r**o**r*

``` r
glm = glm(totaladjusted_vol ~ intersection_uid + leg + dir + day, data = data)
summary(glm)
```

    ## 
    ## Call:
    ## glm(formula = totaladjusted_vol ~ intersection_uid + leg + dir + 
    ##     day, data = data)
    ## 
    ## Deviance Residuals: 
    ##     Min       1Q   Median       3Q      Max  
    ## -6870.3  -1931.9   -335.3   1679.9   9217.9  
    ## 
    ## Coefficients: (1 not defined because of singularities)
    ##                    Estimate Std. Error t value Pr(>|t|)    
    ## (Intercept)         10063.9      439.6  22.892  < 2e-16 ***
    ## intersection_uid2    1887.6      878.8   2.148 0.031880 *  
    ## intersection_uid3     418.8      551.4   0.759 0.447680    
    ## intersection_uid4    2094.5      551.4   3.799 0.000151 ***
    ## intersection_uid5    -310.1      551.4  -0.562 0.573968    
    ## intersection_uid6    1379.8      516.8   2.670 0.007665 ** 
    ## intersection_uid7   -4232.2      516.8  -8.189 5.40e-16 ***
    ## intersection_uid8   -1683.2      622.8  -2.703 0.006952 ** 
    ## intersection_uid9   -4584.4      516.8  -8.871  < 2e-16 ***
    ## intersection_uid10  -1202.6      461.4  -2.606 0.009238 ** 
    ## intersection_uid11  -5571.0      516.8 -10.780  < 2e-16 ***
    ## intersection_uid12    -97.6      532.6  -0.183 0.854623    
    ## intersection_uid13  -5613.7      787.1  -7.132 1.50e-12 ***
    ## intersection_uid14  -4963.9      684.5  -7.252 6.43e-13 ***
    ## intersection_uid15  -1054.6      516.8  -2.041 0.041458 *  
    ## intersection_uid16  -3548.6      684.5  -5.185 2.45e-07 ***
    ## intersection_uid17  -3440.6      516.8  -6.657 3.84e-11 ***
    ## intersection_uid18  -4160.8      516.8  -8.051 1.61e-15 ***
    ## intersection_uid19  -5844.6      516.8 -11.309  < 2e-16 ***
    ## intersection_uid20  -3116.5      466.1  -6.687 3.17e-11 ***
    ## intersection_uid21  -6944.5      516.8 -13.437  < 2e-16 ***
    ## intersection_uid22  -1925.5      493.5  -3.902 9.96e-05 ***
    ## intersection_uid23  -1370.5      516.8  -2.652 0.008084 ** 
    ## intersection_uid24  -3295.8      680.0  -4.847 1.38e-06 ***
    ## intersection_uid25  -2355.9      683.1  -3.449 0.000578 ***
    ## intersection_uid26  -2250.2      551.6  -4.079 4.75e-05 ***
    ## intersection_uid27    727.4      554.0   1.313 0.189379    
    ## intersection_uid28   -759.1      554.0  -1.370 0.170797    
    ## intersection_uid29   1936.3      538.1   3.599 0.000330 ***
    ## intersection_uid31  -1715.1      554.0  -3.096 0.001997 ** 
    ## legN                  882.8      239.2   3.690 0.000232 ***
    ## legS                 1161.0      239.2   4.853 1.34e-06 ***
    ## legW                  -11.3      203.0  -0.056 0.955601    
    ## dirNB                 587.0      184.6   3.180 0.001499 ** 
    ## dirSB                    NA         NA      NA       NA    
    ## dirWB                -161.1      207.1  -0.778 0.436685    
    ## day2                  202.8      253.7   0.799 0.424329    
    ## day3                  620.7      205.1   3.026 0.002515 ** 
    ## day4                  716.9      202.9   3.532 0.000424 ***
    ## day5                 1135.9      239.9   4.734 2.40e-06 ***
    ## ---
    ## Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
    ## 
    ## (Dispersion parameter for gaussian family taken to be 7413679)
    ## 
    ##     Null deviance: 2.2463e+10  on 1603  degrees of freedom
    ## Residual deviance: 1.1602e+10  on 1565  degrees of freedom
    ## AIC: 29966
    ## 
    ## Number of Fisher Scoring iterations: 2

``` r
plot(glm, which = 1)
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-3-1.png)

From the above, we can see many components of the model are statistically significant, with the exception of a few variables.

One aspect missing from the above model is the fact that leg and direction of an intersection are correlated. Let's make an interaction component in the model to account for this fact.

*V**o**l* = *i**n**t**e**r**s**e**c**t**i**o**n**i* + *d**a**y* + *l**e**g* : *d**i**r**e**c**t**i**o**n* + *e**r**r**o**r*

``` r
glm2 = glm(totaladjusted_vol ~ intersection_uid + day + leg:day, data = data)
summary(glm2)
```

    ## 
    ## Call:
    ## glm(formula = totaladjusted_vol ~ intersection_uid + day + leg:day, 
    ##     data = data)
    ## 
    ## Deviance Residuals: 
    ##     Min       1Q   Median       3Q      Max  
    ## -6758.2  -1999.4   -406.3   1784.3   9289.6  
    ## 
    ## Coefficients:
    ##                     Estimate Std. Error t value Pr(>|t|)    
    ## (Intercept)        10010.089    499.731  20.031  < 2e-16 ***
    ## intersection_uid2   1907.114    884.504   2.156 0.031226 *  
    ## intersection_uid3    429.792    554.824   0.775 0.438667    
    ## intersection_uid4   2103.214    554.766   3.791 0.000156 ***
    ## intersection_uid5   -325.974    554.595  -0.588 0.556772    
    ## intersection_uid6   1365.044    519.772   2.626 0.008718 ** 
    ## intersection_uid7  -4247.019    519.772  -8.171 6.27e-16 ***
    ## intersection_uid8  -1702.207    626.462  -2.717 0.006657 ** 
    ## intersection_uid9  -4599.175    519.772  -8.848  < 2e-16 ***
    ## intersection_uid10 -1216.881    464.027  -2.622 0.008816 ** 
    ## intersection_uid11 -5585.785    519.772 -10.747  < 2e-16 ***
    ## intersection_uid12  -112.526    535.688  -0.210 0.833649    
    ## intersection_uid13 -5632.769    791.886  -7.113 1.72e-12 ***
    ## intersection_uid14 -5092.445    688.320  -7.398 2.25e-13 ***
    ## intersection_uid15 -1069.363    519.772  -2.057 0.039817 *  
    ## intersection_uid16 -3481.570    688.320  -5.058 4.74e-07 ***
    ## intersection_uid17 -3455.378    519.772  -6.648 4.10e-11 ***
    ## intersection_uid18 -4175.550    519.772  -8.033 1.86e-15 ***
    ## intersection_uid19 -5859.331    519.772 -11.273  < 2e-16 ***
    ## intersection_uid20 -3130.735    468.744  -6.679 3.34e-11 ***
    ## intersection_uid21 -6959.238    519.772 -13.389  < 2e-16 ***
    ## intersection_uid22 -1940.124    496.353  -3.909 9.68e-05 ***
    ## intersection_uid23 -1385.300    519.772  -2.665 0.007774 ** 
    ## intersection_uid24 -3313.086    684.040  -4.843 1.40e-06 ***
    ## intersection_uid25 -2373.529    687.212  -3.454 0.000567 ***
    ## intersection_uid26 -2266.062    554.824  -4.084 4.65e-05 ***
    ## intersection_uid27   684.667    554.824   1.234 0.217380    
    ## intersection_uid28  -801.812    554.824  -1.445 0.148613    
    ## intersection_uid29  1891.483    538.698   3.511 0.000459 ***
    ## intersection_uid31 -1757.771    554.824  -3.168 0.001564 ** 
    ## day2                 102.087    528.645   0.193 0.846897    
    ## day3                 588.386    419.101   1.404 0.160541    
    ## day4                 803.200    416.099   1.930 0.053750 .  
    ## day5                1008.523    498.648   2.023 0.043294 *  
    ## day1:legN           1235.098    428.982   2.879 0.004042 ** 
    ## day2:legN           1431.722    573.916   2.495 0.012711 *  
    ## day3:legN           1283.670    371.898   3.452 0.000572 ***
    ## day4:legN           1065.632    366.183   2.910 0.003664 ** 
    ## day5:legN           1498.533    518.542   2.890 0.003907 ** 
    ## day1:legS           1550.065    428.982   3.613 0.000312 ***
    ## day2:legS           1638.142    573.916   2.854 0.004370 ** 
    ## day3:legS           1586.488    371.898   4.266 2.11e-05 ***
    ## day4:legS           1368.157    366.183   3.736 0.000194 ***
    ## day5:legS           1687.791    518.542   3.255 0.001159 ** 
    ## day1:legW            -56.643    457.735  -0.124 0.901532    
    ## day2:legW             45.579    609.720   0.075 0.940420    
    ## day3:legW             -7.866    388.094  -0.020 0.983831    
    ## day4:legW            -12.979    383.467  -0.034 0.973005    
    ## day5:legW             15.628    554.513   0.028 0.977520    
    ## ---
    ## Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
    ## 
    ## (Dispersion parameter for gaussian family taken to be 7508123)
    ## 
    ##     Null deviance: 2.2463e+10  on 1603  degrees of freedom
    ## Residual deviance: 1.1675e+10  on 1555  degrees of freedom
    ## AIC: 29996
    ## 
    ## Number of Fisher Scoring iterations: 2

``` r
plot(glm2, which = 1)
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-4-1.png)

<p>
The model is very similar to the previous. The AIC for the second model is only 30 units greater.
<p>
#### ***Multilevel Modelling***

Let's look at box plots of each of our attributes of interest. We suspect there may be some random variation within attributes.

``` r
boxplot(data$totaladjusted_vol ~ data$intersection_uid, xlab = 'Intersection', ylab = 'Volume')
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-5-1.png)

``` r
boxplot(data$totaladjusted_vol ~ data$leg, xlab = 'Leg', ylab = 'Volume')
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-5-2.png)

``` r
boxplot(data$totaladjusted_vol ~ data$dir, xlab = 'Direction', ylab = 'Volume')
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-5-3.png)

``` r
boxplot(data$totaladjusted_vol ~ data$day, xlab = 'Day of Week', ylab = 'Volume')
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-5-4.png)

``` r
boxplot(data$totaladjusted_vol ~ data$dt, xlab = 'Date', ylab = 'Volume')
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-5-5.png)

<p>
From the above, it can be clearly seen that for leg and direction, North/South and NB/SB are greater than East/West and EB/WB. Moreover, for day, it seems as the week progresses, the traffic increases, indicating a clear linear trend.
<p>
<p>
Now consider date and intersection. The variation seems absolutely random, i.e. the difference in traffic volumes do not indicate any clear pattern. This gives us reason to believe that a multilevel component may be at play. Ignoring date, let us consider a multilevel model with `intersection_uid` being the multilevel component, i.e. the intercept for the intersection changes in addition containing a random error component.
<p>
*V**o**l* = *i**n**t**e**r**s**e**c**t**i**o**n*<sub>*i*</sub> + *d**i**r**e**c**t**i**o**n* + *l**e**g* + *d**a**y* + *e**r**r**o**r*

*i**n**t**e**r**s**e**c**t**i**o**n*<sub>*i*</sub> = *β*<sub>0</sub> + *e**r**r**o**r*<sub>*i*</sub>

``` r
ml1 = lmer(totaladjusted_vol ~ dir + leg + day + (1|intersection_uid), data = data)
```

    ## fixed-effect model matrix is rank deficient so dropping 1 column / coefficient

``` r
summary(ml1)
```

    ## Linear mixed model fit by REML ['lmerMod']
    ## Formula: totaladjusted_vol ~ dir + leg + day + (1 | intersection_uid)
    ##    Data: data
    ## 
    ## REML criterion at convergence: 29901.6
    ## 
    ## Scaled residuals: 
    ##     Min      1Q  Median      3Q     Max 
    ## -2.5247 -0.7141 -0.1238  0.6146  3.4130 
    ## 
    ## Random effects:
    ##  Groups           Name        Variance Std.Dev.
    ##  intersection_uid (Intercept) 6114479  2473    
    ##  Residual                     7414136  2723    
    ## Number of obs: 1604, groups:  intersection_uid, 30
    ## 
    ## Fixed effects:
    ##             Estimate Std. Error t value
    ## (Intercept)  8005.34     506.27  15.812
    ## dirNB        1759.50     239.15   7.357
    ## dirSB        1171.30     239.15   4.898
    ## dirWB        -164.19     206.94  -0.793
    ## legN         -278.16     184.00  -1.512
    ## legW          -15.34     202.97  -0.076
    ## day2          206.70     253.69   0.815
    ## day3          616.98     204.86   3.012
    ## day4          714.01     202.77   3.521
    ## day5         1145.48     239.88   4.775
    ## 
    ## Correlation of Fixed Effects:
    ##       (Intr) dirNB  dirSB  dirWB  legN   legW   day2   day3   day4  
    ## dirNB -0.260                                                        
    ## dirSB -0.260  0.702                                                 
    ## dirWB -0.211  0.450  0.450                                          
    ## legN   0.000 -0.385 -0.385  0.000                                   
    ## legW  -0.194  0.409  0.409  0.010  0.000                            
    ## day2  -0.178  0.000  0.000  0.000  0.000  0.000                     
    ## day3  -0.241  0.000  0.000  0.000  0.000  0.000  0.446              
    ## day4  -0.241  0.000  0.000  0.000  0.000  0.000  0.451  0.591       
    ## day5  -0.190  0.000  0.000  0.000  0.000  0.000  0.387  0.475  0.482
    ## fit warnings:
    ## fixed-effect model matrix is rank deficient so dropping 1 column / coefficient

``` r
# the lme4 package doesn't have nice default residual plots, so we make our own
plot(fitted(ml1), residuals(ml1), ylab =  "Residuals") 
abline(h = 0, lty = 2)
lines(lowess(fitted(ml1), residuals(ml1)), col = 'red')
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-6-1.png)

<p>
There seems to be some sort of a sinusoidal pattern in the residuals. Let's try different combinations of this model.
<p>
Recall that legs are a component of each intersection. Rather than just varying the intercept, let's vary the slope as well.

*V**o**l* = (*l**e**g*|*i**n**t**e**r**s**e**c**t**i**o**n*)+*d**i**r* + *l**e**g* + *d**a**y* + *e**r**r**o**r*

``` r
ml2 = lmer(totaladjusted_vol ~ (leg | intersection_uid) + dir + leg + day, data = data)
```

    ## fixed-effect model matrix is rank deficient so dropping 1 column / coefficient

``` r
summary(ml2)
```

    ## Linear mixed model fit by REML ['lmerMod']
    ## Formula: totaladjusted_vol ~ (leg | intersection_uid) + dir + leg + day
    ##    Data: data
    ## 
    ## REML criterion at convergence: 27541
    ## 
    ## Scaled residuals: 
    ##     Min      1Q  Median      3Q     Max 
    ## -3.1074 -0.5197 -0.0215  0.4903  3.6138 
    ## 
    ## Random effects:
    ##  Groups           Name        Variance Std.Dev. Corr             
    ##  intersection_uid (Intercept) 17694909 4207                      
    ##                   legN        23146270 4811     -0.75            
    ##                   legS        24740349 4974     -0.69  0.97      
    ##                   legW         2693210 1641     -0.28  0.05 -0.07
    ##  Residual                      1378626 1174                      
    ## Number of obs: 1604, groups:  intersection_uid, 30
    ## 
    ## Fixed effects:
    ##             Estimate Std. Error t value
    ## (Intercept)  8843.87     775.17  11.409
    ## dirNB         781.94     915.24   0.854
    ## dirSB         233.35     915.24   0.255
    ## dirWB        -450.94      95.63  -4.715
    ## legN         -344.99     226.72  -1.522
    ## legW         -514.97     323.37  -1.593
    ## day2          203.81     109.40   1.863
    ## day3          620.23      88.42   7.015
    ## day4          716.45      87.50   8.188
    ## day5         1138.14     103.46  11.001
    ## 
    ## Correlation of Fixed Effects:
    ##       (Intr) dirNB  dirSB  dirWB  legN   legW   day2   day3   day4  
    ## dirNB -0.687                                                        
    ## dirSB -0.687  0.996                                                 
    ## dirWB -0.064  0.054  0.054                                          
    ## legN  -0.145 -0.252 -0.252  0.000                                   
    ## legW  -0.274 -0.043 -0.043  0.010  0.433                            
    ## day2  -0.050  0.000  0.000  0.000  0.000  0.000                     
    ## day3  -0.068  0.000  0.000  0.000  0.000  0.000  0.446              
    ## day4  -0.068  0.000  0.000  0.000  0.000  0.000  0.451  0.591       
    ## day5  -0.053  0.000  0.000  0.000  0.000  0.000  0.387  0.474  0.482
    ## fit warnings:
    ## fixed-effect model matrix is rank deficient so dropping 1 column / coefficient

``` r
plot(fitted(ml2), residuals(ml2), ylab =  "Residuals") 
abline(h = 0, lty = 2)
lines(lowess(fitted(ml2), residuals(ml2)), col = 'red')
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-7-1.png)

This model seems much better. The sinusoidal pattern in the residuals has decreased. Let's do an anova test to see if this model is better than the varying intercept model from before.

``` r
anova(ml1, ml2) 
```

    ## refitting model(s) with ML (instead of REML)

    ## Data: data
    ## Models:
    ## ml1: totaladjusted_vol ~ dir + leg + day + (1 | intersection_uid)
    ## ml2: totaladjusted_vol ~ (leg | intersection_uid) + dir + leg + day
    ##     Df   AIC   BIC logLik deviance  Chisq Chi Df Pr(>Chisq)    
    ## ml1 12 30051 30115 -15013    30027                             
    ## ml2 21 27703 27816 -13830    27661 2365.7      9  < 2.2e-16 ***
    ## ---
    ## Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1

From the above, it is clear that the second model is significantly better. This difference is statistically significant.

<p>
Instead of including a `leg:dir` interaction component, what if we account for their interaction through a change slope multilevel componnent?

*V**o**l* = (*l**e**g*|*i**n**t**e**r**s**e**c**t**i**o**n*)+(*d**i**r*|*l**e**g*)+*d**i**r* + *l**e**g* + *d**a**y* + *e**r**r**o**r*

``` r
ml3 = lmer(totaladjusted_vol ~ (leg | intersection_uid) + (dir|leg) + dir + leg + day, data = data)
```

    ## fixed-effect model matrix is rank deficient so dropping 1 column / coefficient

    ## Warning in checkConv(attr(opt, "derivs"), opt$par, ctrl = control
    ## $checkConv, : unable to evaluate scaled gradient

    ## Warning in checkConv(attr(opt, "derivs"), opt$par, ctrl = control
    ## $checkConv, : Model failed to converge: degenerate Hessian with 1 negative
    ## eigenvalues

``` r
summary(ml3)
```

    ## Linear mixed model fit by REML ['lmerMod']
    ## Formula: totaladjusted_vol ~ (leg | intersection_uid) + (dir | leg) +  
    ##     dir + leg + day
    ##    Data: data
    ## 
    ## REML criterion at convergence: 27541
    ## 
    ## Scaled residuals: 
    ##     Min      1Q  Median      3Q     Max 
    ## -3.1074 -0.5197 -0.0215  0.4903  3.6138 
    ## 
    ## Random effects:
    ##  Groups           Name        Variance  Std.Dev.  Corr             
    ##  intersection_uid (Intercept) 1.770e+07 4.207e+03                  
    ##                   legN        2.315e+07 4.811e+03 -0.75            
    ##                   legS        2.474e+07 4.974e+03 -0.69  0.97      
    ##                   legW        2.693e+06 1.641e+03 -0.28  0.05 -0.07
    ##  leg              (Intercept) 3.139e+03 5.603e+01                  
    ##                   dirNB       7.540e+05 8.683e+02 -0.99            
    ##                   dirSB       7.540e+05 8.683e+02 -0.99  1.00      
    ##                   dirWB       2.079e-07 4.560e-04 -0.92  0.95  0.95
    ##  Residual                     1.379e+06 1.174e+03                  
    ## Number of obs: 1604, groups:  intersection_uid, 30; leg, 4
    ## 
    ## Fixed effects:
    ##             Estimate Std. Error t value
    ## (Intercept)  8843.87     777.19  11.379
    ## dirNB         781.94    1225.32   0.638
    ## dirSB         233.35    1225.32   0.190
    ## dirWB        -450.94      95.63  -4.715
    ## legN         -344.99    1171.59  -0.294
    ## legW         -514.97     332.94  -1.547
    ## day2          203.81     109.40   1.863
    ## day3          620.23      88.42   7.015
    ## day4          716.45      87.50   8.188
    ## day5         1138.14     103.46  11.001
    ## 
    ## Correlation of Fixed Effects:
    ##       (Intr) dirNB  dirSB  dirWB  legN   legW   day2   day3   day4  
    ## dirNB -0.515                                                        
    ## dirSB -0.515  0.998                                                 
    ## dirWB -0.064  0.040  0.040                                          
    ## legN  -0.028 -0.497 -0.497  0.000                                   
    ## legW  -0.278 -0.024 -0.024  0.010  0.081                            
    ## day2  -0.050  0.000  0.000  0.000  0.000  0.000                     
    ## day3  -0.068  0.000  0.000  0.000  0.000  0.000  0.446              
    ## day4  -0.068  0.000  0.000  0.000  0.000  0.000  0.451  0.591       
    ## day5  -0.053  0.000  0.000  0.000  0.000  0.000  0.387  0.474  0.482
    ## fit warnings:
    ## fixed-effect model matrix is rank deficient so dropping 1 column / coefficient
    ## convergence code: 0
    ## unable to evaluate scaled gradient
    ## Model failed to converge: degenerate  Hessian with 1 negative eigenvalues

``` r
plot(fitted(ml3), residuals(ml3), ylab =  "Residuals") 
abline(h = 0, lty = 2)
lines(lowess(fitted(ml3), residuals(ml3)), col = 'red')
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-9-1.png)

This looks extremely similar to `ml2`. Let's test to see if there are any statistically significant differences.

``` r
anova(ml2, ml3)
```

    ## refitting model(s) with ML (instead of REML)

    ## Data: data
    ## Models:
    ## ml2: totaladjusted_vol ~ (leg | intersection_uid) + dir + leg + day
    ## ml3: totaladjusted_vol ~ (leg | intersection_uid) + (dir | leg) + 
    ## ml3:     dir + leg + day
    ##     Df   AIC   BIC logLik deviance Chisq Chi Df Pr(>Chisq)
    ## ml2 21 27703 27816 -13830    27661                        
    ## ml3 31 27723 27890 -13830    27661     0     10          1

<p>
The changing slope did not change the results at all. In fact, it just increased the AIC.
<p>
<p>
From the above, if multilevel modelling were the chosen approach for predicting traffic volumes, `ml2` would be our best choice.
<p>
####***Multiplicative Model***

<p>
Multilevel modelling was generally better than simple linear regression. Moreover, it reduced the sinusoidal effect on the residuals.
<p>
<p>
We consider a multiplicative approach to the model. This way variables will have more of an impact on the model, i.e. a scaling effect.
<p>
We consider a simple multiplicative model in which we multiply all elements.

*V**o**l* = *I**n**t**e**r**s**e**c**t**i**o**n* \* *D**a**y* \* *L**e**g* \* *D**i**r*

``` r
mult1 = gnm( totaladjusted_vol ~ Mult(intersection_uid, day, leg, dir), data = data)
```

    ## Initialising
    ## Running start-up iterations..
    ## Running main iterations....................................................
    ## ...........................................................................
    ## ...........................................................................
    ## ...........................................................................
    ## ...........................................................................
    ## ...........................................................................
    ## .........................................................................
    ## Done

    ## Warning in gnmFit(modelTools = modelTools, y = y, constrain = constrain, : Fitting algorithm has either not converged or converged
    ## to a non-solution of the likelihood equations.
    ## Use exitInfo() for numerical details of last iteration.

``` r
summary(mult1)
```

    ## 
    ## Call:
    ## gnm(formula = totaladjusted_vol ~ Mult(intersection_uid, day, 
    ##     leg, dir), data = data)
    ## 
    ## Deviance Residuals: 
    ##     Min       1Q   Median       3Q      Max  
    ## -6780.7  -1570.5   -389.2    983.2  12130.7  
    ## 
    ## Coefficients:
    ##                                             Estimate Std. Error t value
    ## (Intercept)                                7.476e+03  1.670e+02   44.75
    ## Mult(., day, leg, dir).intersection_uid1  -2.063e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid2  -2.770e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid3  -1.121e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid4  -2.426e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid5  -2.681e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid6  -4.009e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid7   1.346e+04         NA      NA
    ## Mult(., day, leg, dir).intersection_uid8  -1.697e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid9   8.676e+04         NA      NA
    ## Mult(., day, leg, dir).intersection_uid10 -1.877e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid11  1.595e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid12 -2.984e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid13  1.360e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid14  1.356e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid15 -2.355e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid16 -3.858e+04         NA      NA
    ## Mult(., day, leg, dir).intersection_uid17 -3.168e+04         NA      NA
    ## Mult(., day, leg, dir).intersection_uid18  8.319e+03         NA      NA
    ## Mult(., day, leg, dir).intersection_uid19  1.210e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid20 -1.277e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid21  1.754e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid22 -1.435e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid23 -1.889e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid24 -5.782e+04         NA      NA
    ## Mult(., day, leg, dir).intersection_uid25 -1.482e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid26 -1.486e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid27 -2.399e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid28 -9.088e+04         NA      NA
    ## Mult(., day, leg, dir).intersection_uid29 -2.134e+05         NA      NA
    ## Mult(., day, leg, dir).intersection_uid31 -5.643e+04         NA      NA
    ## Mult(intersection_uid, ., leg, dir).day1  -5.606e-01         NA      NA
    ## Mult(intersection_uid, ., leg, dir).day2  -5.744e-01         NA      NA
    ## Mult(intersection_uid, ., leg, dir).day3  -6.326e-01         NA      NA
    ## Mult(intersection_uid, ., leg, dir).day4  -6.216e-01         NA      NA
    ## Mult(intersection_uid, ., leg, dir).day5  -7.178e-01         NA      NA
    ## Mult(intersection_uid, day, ., dir).legE  -4.181e-02         NA      NA
    ## Mult(intersection_uid, day, ., dir).legN  -2.108e-01         NA      NA
    ## Mult(intersection_uid, day, ., dir).legS  -2.396e-01         NA      NA
    ## Mult(intersection_uid, day, ., dir).legW  -3.167e-02         NA      NA
    ## Mult(intersection_uid, day, leg, .).dirEB -3.242e-01         NA      NA
    ## Mult(intersection_uid, day, leg, .).dirNB -1.800e-01         NA      NA
    ## Mult(intersection_uid, day, leg, .).dirSB -1.389e-01         NA      NA
    ## Mult(intersection_uid, day, leg, .).dirWB -2.652e-01         NA      NA
    ##                                           Pr(>|t|)    
    ## (Intercept)                                 <2e-16 ***
    ## Mult(., day, leg, dir).intersection_uid1        NA    
    ## Mult(., day, leg, dir).intersection_uid2        NA    
    ## Mult(., day, leg, dir).intersection_uid3        NA    
    ## Mult(., day, leg, dir).intersection_uid4        NA    
    ## Mult(., day, leg, dir).intersection_uid5        NA    
    ## Mult(., day, leg, dir).intersection_uid6        NA    
    ## Mult(., day, leg, dir).intersection_uid7        NA    
    ## Mult(., day, leg, dir).intersection_uid8        NA    
    ## Mult(., day, leg, dir).intersection_uid9        NA    
    ## Mult(., day, leg, dir).intersection_uid10       NA    
    ## Mult(., day, leg, dir).intersection_uid11       NA    
    ## Mult(., day, leg, dir).intersection_uid12       NA    
    ## Mult(., day, leg, dir).intersection_uid13       NA    
    ## Mult(., day, leg, dir).intersection_uid14       NA    
    ## Mult(., day, leg, dir).intersection_uid15       NA    
    ## Mult(., day, leg, dir).intersection_uid16       NA    
    ## Mult(., day, leg, dir).intersection_uid17       NA    
    ## Mult(., day, leg, dir).intersection_uid18       NA    
    ## Mult(., day, leg, dir).intersection_uid19       NA    
    ## Mult(., day, leg, dir).intersection_uid20       NA    
    ## Mult(., day, leg, dir).intersection_uid21       NA    
    ## Mult(., day, leg, dir).intersection_uid22       NA    
    ## Mult(., day, leg, dir).intersection_uid23       NA    
    ## Mult(., day, leg, dir).intersection_uid24       NA    
    ## Mult(., day, leg, dir).intersection_uid25       NA    
    ## Mult(., day, leg, dir).intersection_uid26       NA    
    ## Mult(., day, leg, dir).intersection_uid27       NA    
    ## Mult(., day, leg, dir).intersection_uid28       NA    
    ## Mult(., day, leg, dir).intersection_uid29       NA    
    ## Mult(., day, leg, dir).intersection_uid31       NA    
    ## Mult(intersection_uid, ., leg, dir).day1        NA    
    ## Mult(intersection_uid, ., leg, dir).day2        NA    
    ## Mult(intersection_uid, ., leg, dir).day3        NA    
    ## Mult(intersection_uid, ., leg, dir).day4        NA    
    ## Mult(intersection_uid, ., leg, dir).day5        NA    
    ## Mult(intersection_uid, day, ., dir).legE        NA    
    ## Mult(intersection_uid, day, ., dir).legN        NA    
    ## Mult(intersection_uid, day, ., dir).legS        NA    
    ## Mult(intersection_uid, day, ., dir).legW        NA    
    ## Mult(intersection_uid, day, leg, .).dirEB       NA    
    ## Mult(intersection_uid, day, leg, .).dirNB       NA    
    ## Mult(intersection_uid, day, leg, .).dirSB       NA    
    ## Mult(intersection_uid, day, leg, .).dirWB       NA    
    ## ---
    ## Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
    ## 
    ## (Dispersion parameter for gaussian family taken to be 7007815)
    ## 
    ## Std. Error is NA where coefficient has been constrained or is unidentified
    ## 
    ## Residual deviance: 1.096e+10 on 1564 degrees of freedom
    ## AIC: 29877
    ## 
    ## Number of iterations: 500

``` r
plot(mult1, which = 1)
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-11-1.png)

The sinusoidal pattern seems to have reduced. Let's break up the model into two separate components.

*V**o**l* = (*I**n**t**e**r**s**e**c**t**i**o**n* \* *D**a**y*)+(*L**e**g* \* *D**i**r*)

``` r
mult2 = gnm( totaladjusted_vol ~ Mult(intersection_uid, day) + Mult(leg, dir), data = data)
```

    ## Initialising
    ## Running start-up iterations..
    ## Running main iterations....................................................
    ## ...........................................................................
    ## ...........................................................................
    ## ...........................................................................
    ## ...........................................................................
    ## ...........................................................................
    ## .........................................................................
    ## Done

    ## Warning in gnmFit(modelTools = modelTools, y = y, constrain = constrain, : Fitting algorithm has either not converged or converged
    ## to a non-solution of the likelihood equations.
    ## Use exitInfo() for numerical details of last iteration.

``` r
summary(mult2)
```

    ## 
    ## Call:
    ## gnm(formula = totaladjusted_vol ~ Mult(intersection_uid, day) + 
    ##     Mult(leg, dir), data = data)
    ## 
    ## Deviance Residuals: 
    ##     Min       1Q   Median       3Q      Max  
    ## -6873.4  -1915.0   -321.8   1659.0   9188.0  
    ## 
    ## Coefficients:
    ##                                   Estimate Std. Error t value Pr(>|t|)    
    ## (Intercept)                     -1.112e+05  1.048e+01  -10617   <2e-16 ***
    ## Mult(., day).intersection_uid1   2.166e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid2   2.434e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid3   2.227e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid4   2.464e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid5   2.123e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid6   2.367e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid7   1.552e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid8   1.922e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid9   1.502e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid10  1.991e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid11  1.359e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid12  2.151e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid13  1.359e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid14  1.452e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid15  2.013e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid16  1.655e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid17  1.667e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid18  1.563e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid19  1.319e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid20  1.716e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid21  1.159e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid22  1.888e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid23  1.968e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid24  1.688e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid25  1.825e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid26  1.841e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid27  2.273e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid28  2.056e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid29  2.444e+04         NA      NA       NA    
    ## Mult(., day).intersection_uid31  1.918e+04         NA      NA       NA    
    ## Mult(intersection_uid, .).day1   6.624e-01         NA      NA       NA    
    ## Mult(intersection_uid, .).day2   6.730e-01         NA      NA       NA    
    ## Mult(intersection_uid, .).day3   6.957e-01         NA      NA       NA    
    ## Mult(intersection_uid, .).day4   6.997e-01         NA      NA       NA    
    ## Mult(intersection_uid, .).day5   7.234e-01         NA      NA       NA    
    ## Mult(., dir).legE                4.806e+03         NA      NA       NA    
    ## Mult(., dir).legN                3.233e+04         NA      NA       NA    
    ## Mult(., dir).legS                3.241e+04         NA      NA       NA    
    ## Mult(., dir).legW                4.805e+03         NA      NA       NA    
    ## Mult(leg, .).dirEB               2.224e+01         NA      NA       NA    
    ## Mult(leg, .).dirNB               3.351e+00         NA      NA       NA    
    ## Mult(leg, .).dirSB               3.333e+00         NA      NA       NA    
    ## Mult(leg, .).dirWB               2.220e+01         NA      NA       NA    
    ## ---
    ## Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
    ## 
    ## (Dispersion parameter for gaussian family taken to be 7418695)
    ## 
    ## Std. Error is NA where coefficient has been constrained or is unidentified
    ## 
    ## Residual deviance: 1.1595e+10 on 1563 degrees of freedom
    ## AIC: 29969
    ## 
    ## Number of iterations: 500

``` r
plot(mult2, which = 1)
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-12-1.png)

This model isn't much better and still retains that sinusoidal pattern in the residuals.

``` r
mult3 = gnm( totaladjusted_vol ~ Mult(intersection_uid, leg) + dir + day, data = data)
```

    ## Initialising
    ## Running start-up iterations..
    ## Running main iterations....................................................
    ## ...........................................................................
    ## .......................................................................
    ## Done

``` r
summary(mult3)
```

    ## 
    ## Call:
    ## gnm(formula = totaladjusted_vol ~ Mult(intersection_uid, leg) + 
    ##     dir + day, data = data)
    ## 
    ## Deviance Residuals: 
    ##     Min       1Q   Median       3Q      Max  
    ## -7039.9  -1544.4   -437.4   1093.1  12066.5  
    ## 
    ## Coefficients:
    ##                                   Estimate Std. Error t value Pr(>|t|)    
    ## (Intercept)                      7.311e+03  3.773e+02  19.380  < 2e-16 ***
    ## Mult(., leg).intersection_uid1  -8.086e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid2  -1.095e+04         NA      NA       NA    
    ## Mult(., leg).intersection_uid3  -3.308e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid4  -9.484e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid5  -1.036e+04         NA      NA       NA    
    ## Mult(., leg).intersection_uid6  -1.736e+04         NA      NA       NA    
    ## Mult(., leg).intersection_uid7   2.242e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid8  -6.312e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid9   6.128e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid10 -6.682e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid11  9.487e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid12 -1.208e+04         NA      NA       NA    
    ## Mult(., leg).intersection_uid13  8.625e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid14  7.510e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid15 -1.009e+04         NA      NA       NA    
    ## Mult(., leg).intersection_uid16  5.689e+02         NA      NA       NA    
    ## Mult(., leg).intersection_uid17 -6.418e+01         NA      NA       NA    
    ## Mult(., leg).intersection_uid18  1.823e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid19  7.259e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid20 -4.376e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid21  9.979e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid22 -4.640e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid23 -6.718e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid24 -9.612e+02         NA      NA       NA    
    ## Mult(., leg).intersection_uid25 -5.367e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid26 -5.739e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid27 -9.497e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid28 -2.719e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid29 -8.542e+03         NA      NA       NA    
    ## Mult(., leg).intersection_uid31 -1.309e+03         NA      NA       NA    
    ## Mult(intersection_uid, .).legE  -1.591e-01         NA      NA       NA    
    ## Mult(intersection_uid, .).legN  -4.372e-01         NA      NA       NA    
    ## Mult(intersection_uid, .).legS  -5.076e-01         NA      NA       NA    
    ## Mult(intersection_uid, .).legW  -1.090e-01         NA      NA       NA    
    ## dirNB                            7.576e+02  8.325e+02   0.910 0.362957    
    ## dirSB                            2.047e+02  8.324e+02   0.246 0.805805    
    ## dirWB                           -1.377e+02  1.974e+02  -0.698 0.485533    
    ## day2                             1.722e+02  2.471e+02   0.697 0.485856    
    ## day3                             5.715e+02  1.987e+02   2.877 0.004072 ** 
    ## day4                             6.590e+02  1.968e+02   3.348 0.000832 ***
    ## day5                             1.171e+03  2.334e+02   5.018 5.83e-07 ***
    ## ---
    ## Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1
    ## 
    ## (Dispersion parameter for gaussian family taken to be 7044727)
    ## 
    ## Std. Error is NA where coefficient has been constrained or is unidentified
    ## 
    ## Residual deviance: 1.1011e+10 on 1563 degrees of freedom
    ## AIC: 29886
    ## 
    ## Number of iterations: 198

``` r
plot(mult3, which = 1)
```

![](trafficvolume_files/figure-markdown_github/unnamed-chunk-13-1.png)

<p>
This model isn't any better either. If we were to choose a multiplicative model, we should choose `mult1` due to the lowest AIC.
<p>
Conclusion
----------

Some sound starting points to modelling traffic volumes would be `ml2` and `mult1`, whos formulas are respectively as follows:

*V**o**l* = (*l**e**g*|*i**n**t**e**r**s**e**c**t**i**o**n*)+*d**i**r* + *l**e**g* + *d**a**y* + *e**r**r**o**r*

*V**o**l* = *I**n**t**e**r**s**e**c**t**i**o**n* \* *D**a**y* \* *L**e**g* \* *D**i**r*

These are not conclusive results, just solid starting points to approach traffic volumes. The data is quite noisy and sparse, and more data points are definitely needed to accurately predict volume baselines. 1600 data points is too few to build a reliable model. Hoewever, multiplicative and multilevel approaches are great stepping stones.
