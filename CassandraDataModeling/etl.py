{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "# Part I. ETL Pipeline for Pre-Processing the Files"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": []
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "#### Import Python packages "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "# Import Python packages \n",
    "import pandas as pd\n",
    "import cassandra\n",
    "import re\n",
    "import os\n",
    "import glob\n",
    "import numpy as np\n",
    "import json\n",
    "import csv"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "#### Creating list of filepaths to process original event csv data files"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "/home/workspace\n",
      "['/home/workspace/event_data/2018-11-30-events.csv', '/home/workspace/event_data/2018-11-23-events.csv', '/home/workspace/event_data/2018-11-22-events.csv', '/home/workspace/event_data/2018-11-29-events.csv', '/home/workspace/event_data/2018-11-11-events.csv', '/home/workspace/event_data/2018-11-14-events.csv', '/home/workspace/event_data/2018-11-20-events.csv', '/home/workspace/event_data/2018-11-15-events.csv', '/home/workspace/event_data/2018-11-05-events.csv', '/home/workspace/event_data/2018-11-28-events.csv', '/home/workspace/event_data/2018-11-25-events.csv', '/home/workspace/event_data/2018-11-16-events.csv', '/home/workspace/event_data/2018-11-18-events.csv', '/home/workspace/event_data/2018-11-24-events.csv', '/home/workspace/event_data/2018-11-04-events.csv', '/home/workspace/event_data/2018-11-19-events.csv', '/home/workspace/event_data/2018-11-26-events.csv', '/home/workspace/event_data/2018-11-12-events.csv', '/home/workspace/event_data/2018-11-27-events.csv', '/home/workspace/event_data/2018-11-06-events.csv', '/home/workspace/event_data/2018-11-09-events.csv', '/home/workspace/event_data/2018-11-03-events.csv', '/home/workspace/event_data/2018-11-21-events.csv', '/home/workspace/event_data/2018-11-07-events.csv', '/home/workspace/event_data/2018-11-01-events.csv', '/home/workspace/event_data/2018-11-13-events.csv', '/home/workspace/event_data/2018-11-17-events.csv', '/home/workspace/event_data/2018-11-08-events.csv', '/home/workspace/event_data/2018-11-10-events.csv', '/home/workspace/event_data/2018-11-02-events.csv']\n"
     ]
    }
   ],
   "source": [
    "# checking your current working directory\n",
    "print(os.getcwd())\n",
    "\n",
    "# Get your current folder and subfolder event data\n",
    "filepath = os.getcwd() + '/event_data'\n",
    "\n",
    "# Create a for loop to create a list of files and collect each filepath\n",
    "for root, dirs, files in os.walk(filepath):\n",
    "    \n",
    "# join the file path and roots with the subdirectories using glob\n",
    "    file_path_list = glob.glob(os.path.join(root,'*'))\n",
    "    print(file_path_list)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "#### Processing the files to create the data file csv that will be used for Apache Casssandra tables"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "# initiating an empty list of rows that will be generated from each file\n",
    "full_data_rows_list = [] \n",
    "    \n",
    "# for every filepath in the file path list \n",
    "for f in file_path_list:\n",
    "\n",
    "# reading csv file \n",
    "    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: \n",
    "        # creating a csv reader object \n",
    "        csvreader = csv.reader(csvfile) \n",
    "        next(csvreader)\n",
    "        \n",
    " # extracting each data row one by one and append it        \n",
    "        for line in csvreader:\n",
    "            #print(line)\n",
    "            full_data_rows_list.append(line) \n",
    "            \n",
    "# uncomment the code below if you would like to get total number of rows \n",
    "#print(len(full_data_rows_list))\n",
    "# uncomment the code below if you would like to check to see what the list of event data rows will look like\n",
    "#print(full_data_rows_list)\n",
    "\n",
    "# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \\\n",
    "# Apache Cassandra tables\n",
    "csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)\n",
    "\n",
    "with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:\n",
    "    writer = csv.writer(f, dialect='myDialect')\n",
    "    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\\\n",
    "                'level','location','sessionId','song','userId'])\n",
    "    for row in full_data_rows_list:\n",
    "        if (row[0] == ''):\n",
    "            continue\n",
    "        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "6821\n"
     ]
    }
   ],
   "source": [
    "# check the number of rows in your csv file\n",
    "with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:\n",
    "    print(sum(1 for line in f))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "# Part II. Complete the Apache Cassandra coding portion of your project. \n",
    "\n",
    "## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: \n",
    "- artist \n",
    "- firstName of user\n",
    "- gender of user\n",
    "- item number in session\n",
    "- last name of user\n",
    "- length of the song\n",
    "- level (paid or free song)\n",
    "- location of the user\n",
    "- sessionId\n",
    "- song title\n",
    "- userId\n",
    "\n",
    "The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>\n",
    "\n",
    "<img src=\"images/image_event_datafile_new.jpg\">"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "## Begin writing your Apache Cassandra code in the cells below"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "#### Creating a Cluster"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "# This should make a connection to a Cassandra instance your local machine \n",
    "# (127.0.0.1)\n",
    "\n",
    "from cassandra.cluster import Cluster\n",
    "cluster = Cluster()\n",
    "\n",
    "# To establish connection and begin executing queries, need a session\n",
    "session = cluster.connect()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "#### Create Keyspace"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "# TO-DO: Create a Keyspace \n",
    "keyspace_name = \"eventdata\"\n",
    "try:\n",
    "    session.execute(\"\"\"\n",
    "            CREATE KEYSPACE IF NOT EXISTS eventdata\n",
    "            WITH REPLICATION =\n",
    "            { 'class': 'SimpleStrategy', 'replication_factor': 1 }\"\"\"\n",
    "        )\n",
    "except Exception as e:\n",
    "    print(\"Error creating KEYSPACE %s \" % e)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "#### Set Keyspace"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "ename": "SyntaxError",
     "evalue": "invalid syntax (<ipython-input-7-9d90650d9c29>, line 1)",
     "output_type": "error",
     "traceback": [
      "\u001b[0;36m  File \u001b[0;32m\"<ipython-input-7-9d90650d9c29>\"\u001b[0;36m, line \u001b[0;32m1\u001b[0m\n\u001b[0;31m    Connect KEYSPACE to the keyspace specified above\u001b[0m\n\u001b[0m                   ^\u001b[0m\n\u001b[0;31mSyntaxError\u001b[0m\u001b[0;31m:\u001b[0m invalid syntax\n"
     ]
    }
   ],
   "source": [
    "# Connect KEYSPACE to the keyspace specified above\n",
    "try:\n",
    "    session.set_keyspace(\"eventdata\")\n",
    "except Exception as e:\n",
    "    print(\"Error setting KEYSPACE %s \" % e)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "## Create queries to ask the following three questions of the data\n",
    "\n",
    "### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4\n",
    "\n",
    "\n",
    "### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182\n",
    "    \n",
    "\n",
    "### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'\n",
    "\n",
    "\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "## Query 1\n",
    "\n",
    "Find all rows given the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4.\n",
    "\n",
    "For queries to be executed quickly and efficiently we need to define a table that would allow for execution of the following CQL query:\n",
    "```\n",
    "SELECT artist, song title and song's length FROM eventdata WHERE sessionID = 338 AND itemInSession = 4 \n",
    "```\n",
    "\n",
    "Let us now define a table with name *music_library*\n",
    "with *Primary key* set to sessionId, and *Clustering key* itemInSession so that we can efficiently filter by this attributes later on."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "query = \"\"\"CREATE TABLE IF NOT EXISTS music_library (\n",
    "                sessionId int,\n",
    "                itemInSession int,\n",
    "                length double,\n",
    "                artist text,\n",
    "                song text,\n",
    "                userId int,\n",
    "                PRIMARY KEY(sessionId, itemInSession))\"\"\"\n",
    "try:\n",
    "    rows = session.execute(query)\n",
    "except Exception as e:\n",
    "    print(\"Exception occcured on SELECT statement %s\" % e)             "
   ]
  },
  {
   "cell_type": "raw",
   "metadata": {
    "editable": true
   },
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#\n",
    "file = 'event_datafile_new.csv'\n",
    "\n",
    "with open(file, encoding = 'utf8') as f:\n",
    "    csvreader = csv.reader(f)\n",
    "    next(csvreader) # skip header\n",
    "    \n",
    "    for line in csvreader:\n",
    "        # Assign the INSERT statements into the `query` variable\n",
    "        query = \"INSERT INTO music_library (sessionId, itemInSession, length, artist, song, userId)\"\n",
    "        query = query + \" VALUES (%s, %s, %s, %s, %s, %s)\"\n",
    "        ## Assign which column element should be assigned for each column in the INSERT statement.\n",
    "        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`\n",
    "        # let's check that passed line is correct\n",
    "        if len(line) == 11:\n",
    "            artist, fName, gender, itemInSession, lName, length, level, location, sessionId, song, userId = line\n",
    "            t = (int(sessionId), int(itemInSession), float(length), artist, song, int(userId))\n",
    "            #print(t)\n",
    "            session.execute(query,t)\n",
    "        else:\n",
    "            print(line)\n",
    "            raise TypeError(\"not all arguments were provided for string formatting\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "#### SELECT statement to verify the data was entered into the table"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {
    "editable": true,
    "scrolled": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Faithless Music Matters (Mark Knight Dub) 495.3073\n"
     ]
    }
   ],
   "source": [
    "\n",
    "query = \"SELECT artist, song, length FROM music_library WHERE sessionid = 338 AND itemInSession = 4\"\n",
    "try:\n",
    "    rows = session.execute(query)\n",
    "except Exception as e:\n",
    "    print(\"Exception occcured on SELECT statement %s \" % e)\n",
    "    \n",
    "for row in rows:\n",
    "    print(row.artist, row.song, row.length)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "## Query 2:\n",
    "Find rows given only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182.\n",
    "\n",
    "For queries to be executed quickly and efficiently we need to define a table that would allow for execution of the following CQL query:\n",
    "```\n",
    "SELECT itemInSession, artist, song, firstName, lastName FROM played_by_user WHERE userid = 10 AND sessionId = 182\n",
    "```\n",
    "\n",
    "Let us now define a table with name *played_by_user*\n",
    "with composite *Primary key* of (sessionId, userId) and *Clustering key* itemInSession so that results can be ordered by this field."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "query = \"\"\"CREATE TABLE IF NOT EXISTS played_by_user (\n",
    "                firstName text,\n",
    "                lastName text,\n",
    "                sessionId int,\n",
    "                itemInSession int,\n",
    "                artist text,\n",
    "                song text,\n",
    "                userId int,\n",
    "                PRIMARY KEY((sessionId, userId), itemInSession))\"\"\"\n",
    "try:\n",
    "    rows = session.execute(query)\n",
    "except Exception as e:\n",
    "    print(\"Exception occcured on SELECT statement %s\" % e)             "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#\n",
    "file = 'event_datafile_new.csv'\n",
    "\n",
    "with open(file, encoding = 'utf8') as f:\n",
    "    csvreader = csv.reader(f)\n",
    "    next(csvreader) # skip header\n",
    "    \n",
    "    for line in csvreader:\n",
    "        # Assign the INSERT statements into the `query` variable\n",
    "        query = \"INSERT INTO played_by_user (firstName, lastName, sessionId, itemInSession, artist, song, userId)\"\n",
    "        query = query + \" VALUES (%s, %s, %s, %s, %s, %s, %s)\"\n",
    "        # Assign which column element should be assigned for each column in the INSERT statement.\n",
    "        # For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`\n",
    "        if len(line) == 11:\n",
    "            artist, firstName, gender, itemInSession, lastName, length, level, location, sessionId, song, userId = line\n",
    "            t = (fName, lName, int(sessionId), int(itemInSession), artist, song, int(userId))\n",
    "            session.execute(query,t)\n",
    "        else:\n",
    "            print(line)\n",
    "            raise TypeError(\"not all arguments were provided for string formatting\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "0 Down To The Bone Keep On Keepin' On Isaac Valdez\n",
      "1 Three Drives Greece 2000 Isaac Valdez\n",
      "2 Sebastien Tellier Kilometer Isaac Valdez\n",
      "3 Lonnie Gordon Catch You Baby (Steve Pitron & Max Sanna Radio Edit) Isaac Valdez\n"
     ]
    }
   ],
   "source": [
    "query = \"SELECT itemInSession, artist, song, firstName, lastName FROM played_by_user WHERE userid = 10 AND sessionId = 182\"\n",
    "try:\n",
    "    rows = session.execute(query)\n",
    "except Exception as e:\n",
    "    print(\"Exception occcured on SELECT statement %s \" % e)\n",
    "    \n",
    "for row in rows:\n",
    "    print(row.iteminsession, row.artist, row.song, row.firstname, row.lastname)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "## Query 3\n",
    "Find rows for every user name (first and last) in music app history who listened to the song 'All Hands Against His Own'\n",
    "\n",
    "\n",
    "For queries to be executed quickly and efficiently let's define a table based on the following CQL query:\n",
    "```\n",
    "SELECT firstName, lastName FROM songs WHERE song='All Hands Against His Own'\n",
    "```\n",
    "Table name would be *songs*, with *Partition key* song and *Clustering key* userId to identify rows uniquely."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "query = \"\"\"CREATE TABLE IF NOT EXISTS songs (\n",
    "                firstName text,\n",
    "                lastName text,\n",
    "                sessionId int,\n",
    "                gender text,\n",
    "                itemInSession int,\n",
    "                length double,\n",
    "                level text,\n",
    "                artist text,\n",
    "                song text,\n",
    "                userId int,\n",
    "                locationName text,\n",
    "                PRIMARY KEY(song, userId))\"\"\"\n",
    "try:\n",
    "    rows = session.execute(query)\n",
    "except Exception as e:\n",
    "    print(\"Exception occcured on SELECT statement %s\" % e)             \n",
    "\n",
    "                    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "\n",
    "file = 'event_datafile_new.csv'\n",
    "\n",
    "with open(file, encoding = 'utf8') as f:\n",
    "    csvreader = csv.reader(f)\n",
    "    next(csvreader) # skip header\n",
    "    \n",
    "    for line in csvreader:\n",
    "        # Assign the INSERT statements into the `query` variable\n",
    "        query = \"INSERT INTO songs (firstName, lastName, song, userId)\"\n",
    "        query = query + \" VALUES (%s, %s, %s, %s)\"\n",
    "        # Assign which column element should be assigned for each column in the INSERT statement.\n",
    "        if len(line) == 11:\n",
    "            artist, fName, gender, itemInSession, lName, length, level, location, sessionId, song, userId = line\n",
    "            t = (fName, lName, song, int(userId))\n",
    "            session.execute(query,t)\n",
    "        else:\n",
    "            print(line)\n",
    "            raise TypeError(\"not all arguments were provided for string formatting\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Jacqueline Lynch\n",
      "Tegan Levine\n",
      "Sara Johnson\n"
     ]
    }
   ],
   "source": [
    "query = \"SELECT firstName, lastName FROM songs WHERE song='All Hands Against His Own'\"\n",
    "try:\n",
    "    rows = session.execute(query)\n",
    "except Exception as e:\n",
    "    print(\"Exception occcured on SELECT statement %s \" % e)\n",
    "    \n",
    "for row in rows:\n",
    "    print(row.firstname, row.lastname)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "### Drop the tables before closing out the sessions"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "Let's execute a function that drops the table by the provided name"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "def drop_table(name):\n",
    "    query = \"DROP TABLE IF EXISTS %s\" % name\n",
    "    try:\n",
    "        rows = session.execute(query)\n",
    "    except Exception as e:\n",
    "        print(\"Exception occcured on DROP statement \" % e)\n",
    "\n",
    "drop_table(\"music_library\")\n",
    "drop_table(\"played_by_user\")\n",
    "drop_table(\"songs\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "editable": true
   },
   "source": [
    "### Close the session and cluster connection¶"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "session.shutdown()\n",
    "cluster.shutdown()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
