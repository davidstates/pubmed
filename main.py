import urllib.request
import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extensions

__author__ = 'David States'
#
# define a function to fetch the XML file from PubMed using the eutils
#
def doSearch(term):
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term='+term+'&usehistory=y'
    with urllib.request.urlopen(url) as response:
        xd = response.read()
        root = ET.fromstring(xd)
    return(root)

def fetchSearch(webenv, querykey, start, n):
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&query_key='+querykey+'&WebEnv='+webenv+'&retstart='+str(start)+'&retmax='+str(n)
    with urllib.request.urlopen(url) as response:
        xd = response.read()
        root = ET.fromstring(xd)
    return(root)
#
# define a utility function to retrieve the text of an XML node if it exists
# Also escape single quotes so they do not cause problems in SQL or other quotes
#
def getXmlText(root, xpath):
    nd = root.find(xpath)
    if nd is None:
        val = ""
    else:
        tlist = nd.itertext()
        val = ''.join(tlist)
        if val is None:
            val = ""
        val = val.replace("'", "''")

    return(val)
#
# define a utility function to excute and SQL statement
#
class MyCursor(psycopg2.extensions.cursor):
    def executesql(self, statement):
        try:
            self.execute(statement)
        except:
            print("Unable to execute SQL")
            print(statement)
            exit()
#
# Open the database connection
#
conn = None
try:
    conn = psycopg2.connect("dbname=affigen user=dbuser host=affigenresearch0.ccq2bhqzswxn.us-east-1.rds.amazonaws.com password=affigen")
except:
    print("Unable to connect to the database")
    exit()

# Create a cursor for the database connection
try:
    cur = conn.cursor(cursor_factory=MyCursor)
except:
    print("Unable to create cursor")
    conn.close()
    exit()

# Set lit as the default schema
sql = 'SET search_path TO lit, public'
cur.executesql(sql)
#
# Query for PubMed
# Hint on specifying terms: Do the search in PubMed, then grab the URL from your browser
#
# Clinical trial searches
#searchterm='(leukemia)+AND+(%22clinical+trial%22+OR+clinical+trial+%5Bpt%5D)'
#searchterm='(lymphoma)+AND+(%22clinical+trial%22+OR+clinical+trial+%5Bpt%5D)'
#searchterm='(myeloma)+AND+(%22clinical+trial%22+OR+clinical+trial+%5Bpt%5D)'
searchterm='(lymphoma+OR+leukemia+OR+myeloma)+AND+(%22clinical+trial%22+OR+clinical+trial+%5Bpt%5D)'
#
# Drug therapy searches
#searchterm="%22Lymphoma%2C+B-Cell%2Fdrug+therapy%22%5BMAJR%5D"
#searchterm="%22Lymphoma%2C+Large+B-Cell%2C+Diffuse%2Fdrug+therapy%22%5Bmajr%5D"
#searchterm="%22Lymphoma%2C+mantle+Cell%2Fdrug+therapy%22%5Bmajr%5D"
#searchterm="%22Lymphoma,+Follicular/drug+therapy%22%5Bmajr%5D"
#searchterm="%22Leukemia,+Lymphocytic,+Chronic,+B-Cell/drug+therapy%22[MAJR]"
#
#searchterm='non+hodgkin+lymphoma+%5Bmajr%5D+OR+chronic+lymphocytic+leukemia+%5Bmajr%5D'
#
# A ~50 paper test set
#searchterm = 'lymphoma+AND+clinical+trial+%5Bpt%5D+AND+2019+%5Bdp%5D'

result = doSearch(searchterm)
webenv = result.find('./WebEnv').text
querykey = result.find('./QueryKey').text
count = int(result.find('./Count').text)

retchunk = 200
retmax = count
print("Found "+str(retmax) + " citations.")

icount=0

for start in range(0, retmax, retchunk):
    result = fetchSearch(webenv, querykey, start, retchunk)
    for article in result.findall('./PubmedArticle'):
        icount += 1
        pmid = article.find('./MedlineCitation/PMID').text
        journal = getXmlText(article, './MedlineCitation/Article/Journal/Title')
        isojournal = getXmlText(article, './MedlineCitation/Article/Journal/ISOAbbreviation')
        volume = getXmlText(article, './MedlineCitation/Article/Journal/JournalIssue/Volume')
        issue = getXmlText(article, './MedlineCitation/Article/Journal/JournalIssue/Issue')
        pubyear = getXmlText(article, './MedlineCitation/Article/Journal/JournalIssue/PubDate/Year')
        pubmonth = getXmlText(article, './MedlineCitation/Article/Journal/JournalIssue/PubDate/Month')
        pubday = getXmlText(article, './MedlineCitation/Article/Journal/JournalIssue/PubDate/Day')
        page = getXmlText(article, './MedlineCitation/Article/Pagination/MedlinePgn')
        year = getXmlText(article, './MedlineCitation/Article/ArticleDate/Year')
        month = getXmlText(article, './MedlineCitation/Article/ArticleDate/Month')
        day = getXmlText(article, './MedlineCitation/Article/ArticleDate/Day')
        title = getXmlText(article, './MedlineCitation/Article/ArticleTitle')
        lang = getXmlText(article, './MedlineCitation/Article/Language')
        sql="select max(id) from pub where pmid='" + pmid + "'"
        cur.executesql(sql)
        qret = cur.fetchone()
        if qret[0] is None:
            print("PMID:" + pmid + " loading " + str(icount) )
            sql = "INSERT into pub(pmid, journal, isojournal, volume, issue, pubyear, pubmonth, pubday, page, year, month, day, title, language)"
            sql = sql+" VALUES('"+pmid+"','"+journal+"','"+isojournal+"','"+volume+"','"+issue+"','"+pubyear+"','"+pubmonth+"','"+pubday+"','"+page+"','"+year+"','"+month+"','"+day+"','"+title+"','" + lang +"')"
            cur.executesql(sql)
            sql = "select max(id) from pub where pmid='" + pmid + "'"
            cur.executesql(sql)
            qret = cur.fetchone()
            pubid=str(qret[0])
        else:
            pubid=str(qret[0])
            #
            # For performance reasons, if the pub table is loaded, don't try to see if any new data is present in the XML.
            # Sometimes NLM does update PubMed records. If this continue is removed, the program will go through each element of the XML and try to load it.
            #
            print("PMID:"+pmid+" already loaded " + str(icount))

        newdata = False
        for nd in article.findall('./PubmedData/History/PubMedPubDate'):
            status = nd.attrib['PubStatus']
            year = getXmlText(nd, './Year')
            month = getXmlText(nd, './Month')
            day = getXmlText(nd, './Day')
            sql = "SELECT max(id) from status where pubid=" + pubid + " and status='" + status + "' and year='" + year + "' and month='" + month + "' and day='" + day + "'"
            cur.executesql(sql)
            qret = cur.fetchone()
            if qret[0] is None:
                newdata = True
                print("Found new status "+status)
                sql = "INSERT INTO status(pubid, status, year, month, day) VALUES(" + pubid + ", '" + status + "','" + year + "','" + month + "','" + day +"')"
                cur.executesql(sql)

        if not newdata:
            continue

        sql = "SELECT max(id) from abstract where pubid="+pubid
        cur.executesql(sql)
        qret = cur.fetchone()
        if qret[0] is None:
            abstract = ''
            for term in article.findall('./MedlineCitation/Article/Abstract/AbstractText'):
                section = getXmlText(term, ".")
                if section is None:
                    section = ''
                if 'Label' in term.attrib:
                    seclabel = term.attrib['Label']
                    seclabel = seclabel.replace("'", "''")
                else:
                    seclabel = ''
                if 'NlmCategory' in term.attrib:
                    seccat = term.attrib['NlmCategory']
                else:
                    seccat = ''
                abstract = abstract + ' ' + section
                sql = "INSERT INTO abstract(pubid, seclabel, nlmcategory, abstract) "
                sql = sql + "VALUES("+pubid+",'" + seclabel + "','" + seccat + "','" + section + "')"
                cur.executesql(sql)

        for author in article.findall('./MedlineCitation/Article/AuthorList/Author'):
            lastname = getXmlText(author, './LastName')
            forename = getXmlText(author, './ForeName')
            initials = getXmlText(author, './Initials')
            collective = getXmlText(author, './CollectiveName')

            sql = "SELECT max(id) from author where lastname='" + lastname + "' and forename='" + forename + "' and initials='"+ initials + "' and collectivename='" + collective +"'"
            cur.executesql(sql)
            qret = cur.fetchone()
            if qret[0] is None:
                sql = "INSERT INTO author(lastname, forename, initials, collectivename) VALUES('" + lastname +"','"+ forename + "','" + initials + "','" + collective + "')"
                cur.executesql(sql)
                sql = "SELECT max(id) from author where lastname='" + lastname + "' and forename='" + forename + "' and initials='" + initials + "' and collectivename='" + collective + "'"
                cur.executesql(sql)
                qret = cur.fetchone()
            authorid = str(qret[0])
            sql = "SELECT max(id) from pubauthor where pubid=" + pubid + " and authorid=" + authorid
            cur.executesql(sql)
            qret = cur.fetchone()
            if qret[0] is None:
                sql = "INSERT INTO pubauthor(pubid, authorid) VALUES(" + pubid +"," + authorid + ")"
                cur.executesql(sql)

            for af in author.findall('./AffiliationInfo/Affiliation'):
                affiliation = getXmlText(af, '.')
                sql = "SELECT max(id) from authoraffiliation where authorid=" + authorid + " and affiliation='" + affiliation + "'"
                cur.executesql(sql)
                qret = cur.fetchone()
                if qret[0] is None:
                    sql = "INSERT INTO authoraffiliation(authorid, affiliation) VALUES(" + authorid + ",'" + affiliation + "')"
                    cur.executesql(sql)

            for id in author.findall('./Identifier'):
                identifier = getXmlText(id, '.')
                source = id.attrib['Source']
                sql = "SELECT max(id) from authoridentifier where authorid=" + authorid + " and source='" + source + "' and identifier='" + identifier + "'"
                cur.executesql(sql)
                qret = cur.fetchone()
                if qret[0] is None:
                    sql = "INSERT INTO authoridentifier(authorid, source, identifier) VALUES(" + authorid + ",'" + source + "','" + identifier + "')"
                    cur.executesql(sql)

        for term in article.findall('./MedlineCitation/Article/DataBankList/DataBank'):
            dbname = getXmlText(term, './DataBankName')
            for acc in term.findall('./AccessionNumberList/AccessionNumber'):
                accession = getXmlText(acc, '.')
                sql = "SELECT max(id) from pubdata where pubid="+pubid+" AND source='"+dbname+"' and accession='"+accession+"'"
                cur.executesql(sql)
                qret=cur.fetchone()
                if qret[0] is None:
                    sql = "INSERT into pubdata(pubid, source, accession) VALUES("+pubid+",'"+dbname+"','"+accession+"')"
                    cur.executesql(sql)

        for term in article.findall('./MedlineCitation/Article/GrantList/Grant'):
            grantid = getXmlText(term, './GrantID')
            agency = getXmlText(term, './Agency')
            country = getXmlText(term, './Country')
            sql = "SELECT max(id) from pubfunding where pubid="+pubid+" and grantid='"+grantid+"' and agency='"+agency+"' and country='"+country+"'"
            cur.executesql(sql)
            qret = cur.fetchone()
            if qret[0] is None:
                sql = "INSERT INTO pubfunding(pubid, grantid, agency, country) VALUES("+pubid+",'"+grantid+"','"+agency+"','"+country+"')"
                cur.executesql(sql)

        for term in article.findall('./MedlineCitation/ChemicalList/Chemical/NameOfSubstance'):
            chemui = term.attrib['UI']
            chemical = getXmlText(term, '.')
            sql = "SELECT max(id) from pubchemical where pubid="+pubid+" and chemui='"+chemui+"' and chemical='"+chemical+"'"
            cur.executesql(sql)
            qret=cur.fetchone()
            if qret[0] is None:
                sql="INSERT into pubchemical(pubid, chemui, chemical) VALUES("+pubid+",'"+chemui+"','"+chemical+"')"
                cur.executesql(sql)

        for term in article.findall('./MedlineCitation/KeywordList'):
            keyword = getXmlText(term, './Keyword')
            #
            # Sometimes NLM combines author supplied keywords using a '; ' delimiter
            # Break these into individal keywords
            #
            for kw in keyword.split('; '):
                sql="SELECT max(id) from pubkeyword where pubid="+pubid+" and keyword='"+kw+"'"
                cur.executesql(sql)
                qret=cur.fetchone()
                if qret[0] is None:
                    sql="INSERT INTO pubkeyword(pubid, keyword) VALUES("+pubid+",'"+kw+"')"
                    cur.executesql(sql)

        for term in article.findall('./MedlineCitation/MeshHeadingList/MeshHeading'):
            dt = term.find('./DescriptorName')
            mesh = getXmlText(dt, ".")
            mesh = mesh.replace("'", "''")
            major = dt.attrib['MajorTopicYN']
            mui = dt.attrib['UI']
            sql = "SELECT max(id) from pubmesh where pubid=" + pubid + " and term='" + mesh + "'"
            cur.executesql(sql)
            qret=cur.fetchone()
            if qret[0] is None:
                sql = "INSERT INTO pubmesh(pubid, meshid, term, major) VALUES(" + pubid + ",'" + mui + "','" + mesh + "','" + major + "')"
                cur.executesql(sql)
                sql = "SELECT max(id) from pubmesh where pubid=" + pubid + " and term='" + mesh + "'"
                cur.executesql(sql)
                qret = cur.fetchone()
            pubmeshid = str(qret[0])

            for qt in term.findall('./QualifierName'):
                qual = getXmlText(qt, ".")
                qmaj = qt.attrib['MajorTopicYN']
                sql="SELECT max(id) from pubmeshqualifier WHERE pubid=" + pubid + " and pubmeshid=" + pubmeshid + " and qualifier='" + qual + "'"
                cur.executesql(sql)
                qret = cur.fetchone()
                if qret[0] is None:
                    sql = "INSERT INTO pubmeshqualifier(pubid, pubmeshid, qualifier, major) VALUES (" + pubid + "," + pubmeshid + ",'" + qual + "', '" + qmaj + "')"
                    cur.executesql(sql)

        for pt in article.findall('./MedlineCitation/Article/PublicationTypeList/PublicationType'):
            pubtype = getXmlText(pt,".")
            typeid = pt.attrib['UI']
            sql = "SELECT max(id) from pubtype where pubid="+pubid+" and typeid='" + typeid + "' and pubtype='" + pubtype + "'"
            cur.executesql(sql)
            qret = cur.fetchone()
            if qret[0] is None:
                sql = "INSERT INTO pubtype(pubid, typeid, pubtype) VALUES(" + pubid + ",'" + typeid + "','" + pubtype +"')"
                cur.executesql(sql)

        for term in article.findall('./PubmedData/ArticleIdList/ArticleId'):
            id = getXmlText(term, ".")
            idtype = term.attrib['IdType']
            sql = "SELECT max(id) from pubidentifier where pubid=" + pubid + " and idtype='" + idtype + "' and identifier='" + id + "'"
            cur.executesql(sql)
            qret = cur.fetchone()
            if qret[0] is None:
                sql = "INSERT INTO pubidentifier(pubid, idtype, identifier) VALUES(" + pubid + ",'" + idtype + "','" + id + "')"
                cur.executesql(sql)

        for term in article.findall('./PubmedData/ReferenceList/Reference'):
            citation = getXmlText(term, './Citation')
            for id in term.findall('./ArticleIdList/ArticleId'):
                citetype = id.attrib['IdType']
                citeid = getXmlText(id, '.')
                sql = "SELECT max(id) FROM pubreference where pubid=" + pubid + " and citetype='" + citetype + "' and citeid='" + citeid + "'"
                cur.executesql(sql)
                qret = cur.fetchone()
                if qret[0] is None:
                    sql = "INSERT INTO pubreference(pubid, citetype, citeid) VALUES(" + pubid + ",'" + citetype + "','" + citeid + "')"
                    cur.executesql(sql)

        conn.commit()

conn.close