# hadoop-dns-mining

This is a small framework for performing large amounts of DNS lookups using Hadoop. This is a work in progress, pull requests are welcome.

## Here are the steps for getting it working:

### Download, compile and install the Maxmind JAR into maven

    
    wget http://geolite.maxmind.com/download/geoip/api/java/GeoIPJava-1.2.5.zip
    unzip GeoIPJava-1.2.5.zip
    cd GeoIPJava-1.2.5/source/com/maxmind/geoip/
    javac *.java
    cd ../../../
    zip -r maxmind.jar com/
    mvn install:install-file -Dfile=maxmind.jar -DgroupId=com.maxmind -DartifactId=geo-ip -Dversion=1.2.5 -Dpackaging=jar
    

### Obtain the Maxmind IP Geo Database

    
    wget http://geolite.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz
    gzip -d GeoLiteCity.dat.gz
    

### Obtain the Maxmind ASN Database

    
    wget http://www.maxmind.com/download/geoip/database/asnum/GeoIPASNum.dat.gz
    gzip -d GeoIPASNum.dat.gz
    

### Create/obtain large lists of domain names (e.g. domains.txt) and copy them into HDFS
    
    # you may want to split these domain files before placing in HDFS in order to use more mappers
    split -a 5 -d -l 100000  domains.txt domains_
    hadoop fs -put domains_* /data/domains/
    

### Download and build this project

    
    git clone https://jt6211@github.com/jt6211/hadoop-dns-mining.git
    cd hadoop-dns-mining
    mvn package assembly:assembly
    

### Run the various MapReduce jobs

    
    # These are the records that will be requested
    REC_TYPES=A,MX,NS,TXT
    
    JAR=target/hadoop-dns-mining-1.0-SNAPSHOT-job.jar
    
	# performs A record, MX record, and NS record lookups on each domain provided using 50 
    # resolving threads per Mapper using the nameserver of 8.8.8.8 and store the results in 
    # HDFS in /data/dns-mining/01_raw
	# Note: choose the nameserver wisely, otherwise you may overload it.  In testing I mainly 
    #  used a bind server deployed on each hadoop node so my nameserver was 127.0.0.1
    time hadoop jar $JAR io.covert.dns.collection.CollectionJob \
        -D dns.collection.num.resolvers=50 \
        -D dns.collection.nameservers=8.8.8.8 \
        IN \
        "$REC_TYPES" \
        /data/domains/ \
        /data/dns-mining/01_raw
    
    # parse the raw responses into JSON (one record per RR in the DNS responses)
    time hadoop jar $JAR io.covert.dns.parse.ParseJob \
        /data/dns-mining/01_raw \
        /data/dns-mining/02_parsed
    
    # lookup any IP addresses in the results in the maxmind DBs and enrich the records
    time hadoop jar $JAR io.covert.dns.geo.GeoJob \
        -files /usr/local/lib/maxmind/GeoLiteCity.dat,/usr/local/lib/maxmind/GeoIPASNum.dat \
        GeoLiteCity.dat \
        GeoIPASNum.dat \
        /data/dns-mining/02_parsed  \
        /data/dns-mining/03_enriched
    
    # run a filter job for the rec types requested as well as for rec types that commonly occur in 
    # the results as part of normal queries.  This will separate the various DNS records into their
    # own directories in HDFS
    for X REC `echo "$REC_TYPES,SOA,NS,CNAME" | sed 's/,/\n/g'| sort -u`; 
    do
        time hadoop jar $JAR io.covert.dns.filtering.FilterJob \
            "type == '$REC'" \
            /data/dns-mining/03_enriched \
            /data/dns-mining/04_filtered-type=$REC; 
    done
    
    # This is a JEXL expression that filters out target fields that are IP addresses 
    # and returns the target field lowercased
    TARGET_EXPR='if(target !~ "^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\.$")return target.toLowerCase()'
    
    # extract the 'target' field from the MX records
    time hadoop jar $JAR io.covert.dns.extract.ExtractorJob "$TARGET_EXPR" \
        /data/dns-mining/04_filtered-type=MX /data/dns-mining/05_extracted-mailservers
    
    # extract the 'target' field from the NS records
    time hadoop jar $JAR io.covert.dns.extract.ExtractorJob "$TARGET_EXPR" \
        /data/dns-mining/04_filtered-type=NS /data/dns-mining/05_extracted-nameservers
    
    HOST_EXPR='if(host !~ "^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\.$")return host.toLowerCase()'
    
    # extract the 'host' field from the SOA records
    time hadoop jar $JAR io.covert.dns.extract.ExtractorJob "$HOST_EXPR" \
        /data/dns-mining/04_filtered-type=SOA /data/dns-mining/05_extracted-nameservers-SOA
    

