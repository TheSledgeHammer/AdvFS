$ =======================================================================
$   (c) Copyright Hewlett-Packard Development Company, L.P., 2008
$
$   This program is free software: you can redistribute it and/or modify
$   it under the terms of version 2 the GNU General Public License as
$   published by the Free Software Foundation.
$   
$   This program is distributed in the hope that it will be useful,
$   but WITHOUT ANY WARRANTY; without even the implied warranty of
$   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
$   GNU General Public License for more details.
$   
$   You should have received a copy of the GNU General Public License
$   along with this program.  If not, see <http://www.gnu.org/licenses/>.
$
$ =======================================================================

$ *****************************************************************
$ *                                                               *
$ *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
$ *                                                               *
$ *****************************************************************
$
$ HISTORY
$ 
$ 

$ /*
$  * X/OPEN message catalogue
$  */

$quote "

$set S_VRESTORE1
VRESTORE1	"%s  Version %s\n"
VRESTORE2	"\n%s: invalid parameter for option <-o>\n"
VRESTORE3	"\n%s: missing parameter for option <-f>\n"
VRESTORE4	"%s: too many files (%d) to extract;\n"
VRESTORE5	"%s: error accessing target directory <%s>; [%d] %s\n"
VRESTORE6	"%s: cannot create block pool\n"
VRESTORE7	"%s: file extraction list is invalid\n"
VRESTORE8	"%s: state transition after 'end of vol set'\n"
VRESTORE9	"Reached end of file"
VRESTORE10	"Read a bad block"
VRESTORE11	"%s: unexpected error from rblk_get(): [%d] %s\n"
VRESTORE12	"%s: unable to restore block <%d>; corrupt block\n"
VRESTORE13	"%s: empty save-set\n"
VRESTORE14	"%s: cannot access '/'\n"
VRESTORE15	"%s: nothing will be restored\n"
VRESTORE16	"%s: encountered unexpected block type; terminating\n"
VRESTORE17	"%s: *** bad save-set ***, "
VRESTORE18	"record size <%d> in block <%d> is bad, "
VRESTORE19	"*** cannot restore this block ***\n"
VRESTORE20	"directory header"
VRESTORE21	"directory data"
VRESTORE22	"filler"
VRESTORE23	"end of save-set"
VRESTORE24	"unknown/unexpected"
VRESTORE25	"ACL"
VRESTORE26	"%s warning: ACLs not supported on destination file system\n"
VRESTORE28	"directory attributes"
VRESTORE29	"hard link attributes"
VRESTORE30	"sym link attributes"
VRESTORE31	"device attributes"
VRESTORE32	"file attributes"
VRESTORE34	"file data"
VRESTORE35	"%s: unexpected mode [%d]; terminating\n"
VRESTORE36	"Usage:\n\n"
VRESTORE37	"%s -h\n"
VRESTORE38	"%s -V\n"
VRESTORE39	"%s -t [-f device]\n"
VRESTORE40	"%s -l [-Q] [-f device]\n"
VRESTORE41	"%s -i [-mqQv] [-f device] [-D pathname] [-o opt]\n"
VRESTORE42	"%s -x [-mqQv] [-f device] [-D pathname] [-o opt] [filename ...]\n"
VRESTORE43	"Options:\n"
VRESTORE44	"   -D <pathname>\n"
VRESTORE45	"\tSpecifies the destination path of where to restore\n"
VRESTORE46	"\tthe files.\n"
VRESTORE47	"   -V \n"
VRESTORE48	"\tDisplays the current %s version.\n"
VRESTORE49	"   -f <device> \n"
VRESTORE50	"\tSpecifies the save set device or file.\n"
VRESTORE51	"   -h \n"
VRESTORE52	"\tDisplays usage help for %s.\n"
VRESTORE53	"   -i \n"
VRESTORE54	"\tRuns the interactive shell to select files.\n"
VRESTORE55	"   -l \n"
VRESTORE56	"\tLists the save set structure.\n"
VRESTORE57	"   -m \n"
VRESTORE58	"\tDoes not preserve owner, group, or modes from device.\n"
VRESTORE59	"   -o <yes | no | ask> \n"
VRESTORE60	"\tSpecifies the action to take when file already exists.\n"
VRESTORE61	"\tThe default is 'yes' (overwrite existing files).\n"
VRESTORE62	"   -q \n"
VRESTORE63	"\tDoes not display unnecessary, informative messages.\n"
VRESTORE68	"   -t \n"
VRESTORE69	"\tShow the files in the save set.\n"
VRESTORE70	"   -v \n"
VRESTORE71	"\tDisplays the names of files being restored.\n"
VRESTORE72	"   -x [other options] [filename ...] \n"
VRESTORE73	"\tSpecifies the files to restore.\n"
VRESTORE74	"\nXOR BLOCK HEADER\n"
VRESTORE75	"\nBLOCK HEADER\n"
VRESTORE76	" block size : %d\n"
VRESTORE77	" flags      : "
VRESTORE78	"none"
VRESTORE79	"<compressed>"
VRESTORE80	"<XOR block>"
VRESTORE81	"<files>"
VRESTORE82	"<dirs>"
VRESTORE83	" volume set : %d\n"
VRESTORE84	" volume sets: %d\n"
VRESTORE85	" volume num : %d\n"
VRESTORE86	" block num  : %d\n"
VRESTORE87	" header CRC : 0x%04x\n"
VRESTORE88	" block CRC  : 0x%04x\n"
VRESTORE89	" xor blocks : %d\n"
VRESTORE90	" xor blk num: %d\n"
VRESTORE91	" id         : %s"
VRESTORE92	" version    : %s\n"
VRESTORE93	"\nRECORD HEADER (%s)\n"
VRESTORE94	" record type  : %d\n"
VRESTORE96	" data offset  : %d\n"
VRESTORE97	" record size  : %d\n"
VRESTORE98	" flags        : "
VRESTORE99	"<none>"
VRESTORE100	"<data-continued>"
VRESTORE101A	" file name : %s\n"
VRESTORE101B	" file name : "
VRESTORE102	" file inode: %d\n"
VRESTORE103	" file mode : 0%o\n"
VRESTORE104	" file owner: %d\n"
VRESTORE105	" file group: %d\n"
VRESTORE106	" file links: %u\n"
VRESTORE107	" file size : %ld\n"
VRESTORE108	" parent ino: %d\n"
VRESTORE109A	"      link name : %s --> %s\n"
VRESTORE109B	"      link name : "
VRESTORE110	"     link inode : %d\n"
VRESTORE111	" link parent ino: %d\n"
VRESTORE112	" file parent ino: %d\n"
VRESTORE113A	" link name : %s @-> %s\n"
VRESTORE113B	" link name : "
VRESTORE114	"     inode : %d\n"
VRESTORE115	"%s: unable to use save-set; invalid or corrupt format\n"
VRESTORE118	"%s: Need %s V%s to restore contents; terminating\n"
VRESTORE119	"%s: Date of the advdump save-set: %s"
VRESTORE120	"%s: unable to create directory <%s>; [%d] %s\n"
VRESTORE121	"%s: %s not restored; file already exists\n"
VRESTORE122	"%s: <%s> already exists; overwrite? "
VRESTORE123	"%s: %s not restored.\n"
VRESTORE124	"\n%s: <%s>; Out of disk space\n"
VRESTORE125	"%s: close file fstat(%s) failed; [%d] %s\n"
VRESTORE126	"%s: extending file <%s> from %li to %li\n"
VRESTORE127	"%s: close file lseek(%s, %li) failed; [%d] %s\n"
VRESTORE128	"%s: incomplete file <%s> restored as <%s>\n"
VRESTORE129	"utimes"
VRESTORE130	"%s: cannot rename temp file <%s> to <%s>; [%d] %s\n"
VRESTORE131A	" dir name  : %s/\n"
VRESTORE131B	" dir name  : "
VRESTORE132	" dir inode : %d\n"
VRESTORE133	" dir mode  : 0%o\n"
VRESTORE134	" dir owner : %d\n"
VRESTORE135	" dir group : %d\n"
VRESTORE136	"%s: unable to create logical dir <%s>; errno [%d]\n"
VRESTORE137	"%s: cannot get path for dir inode <%d>, dir <%s>\n"
VRESTORE138	"r  %s/%s/\n"
VRESTORE139	"%s: unable to set bitfile attributes for dir <%s>; %s\n"
VRESTORE140	"%s: unable to set inheritable attributes for dir <%s>; %s\n"
VRESTORE141A	" dev name  : %s\n"
VRESTORE141B	" dev name  : "
VRESTORE142	" dev inode : %d\n"
VRESTORE143	" device    : 0%o\n"
VRESTORE144	" dev mode  : 0%o\n"
VRESTORE145	" dev owner : %d\n"
VRESTORE146	" dev group : %d\n"
VRESTORE147	"%s: unable to create device file <%s>; [%d] %s\n"
VRESTORE148	"%s: cannot get path for dir inode <%d>, file <%s>\n"
VRESTORE149	"r  %s/%s --> %s/%s\n"
VRESTORE150	"%s: error creating link %s --> %s; [%d] %s\n"
VRESTORE151	"r  %s/%s @-> %s\n"
VRESTORE152	"%s: error creating symbolic link %s @-> %s; [%d] %s\n"
VRESTORE153	"r  %s/"
VRESTORE154	"%s: unable to create file <%s>; [%d] %s\n"
VRESTORE155	"\n%s: Error writing to file; [%d] %s\n"
VRESTORE156	"%s: lseek error; [%d] %s\n"
VRESTORE157	"%s: unable to open save-set <%s>; [%d] %s\n"
VRESTORE158	"%s: device <%s> is OFFLINE\n"
VRESTORE161	"%s: unable to read from save-set; [%d] %s\n"
VRESTORE162	"%s: unable to read from save-set; incomplete read %d < %d\n"
VRESTORE163	"%s: unable to reset save-set; [%d] %s\n"
VRESTORE164	"\n%s: unable to read from device <%s>; [%d] %s\n"
VRESTORE165	"%s: unable to get device <%s> status; [%d] %s\n"
VRESTORE166	"%s: unable to read from device '%s'; incomplete read %d < %d\n"
VRESTORE167	"%s: unable to rewind tape; [%d] %s\n"
VRESTORE168	"\n%s: Do you want to abort the restore? (yes or no) "
VRESTORE169     "%s: Cannot use interactive shell in batch mode\n"
VRESTORE170     "%s: user interaction required and no tty present\n"
VRESTORE171	"%s: Could not obtain %s quota file name: [%d] %s.\nQuotas not included in saveset may not be removed.\n"
VRESTORE172	"%s: Must be root user to restore quota information.\n"
VRESTORE173	"%s: %s quotas must be enabled in order to restore %s quota file.\n"
VRESTORE174	"%s: Open of %s quota file failed: [%d] %s.\n%s quotas cannot be restored.\n"
VRESTORE175	"%s: Could not obtain fileset parameters: [%d] %s\nFileset quotas cannot be restored.\n"
VRESTORE176	"<quotas>"
VRESTORE177	"day"
VRESTORE178	"days"
VRESTORE179	"hour"
VRESTORE180	"hours"
VRESTORE181	"minute"
VRESTORE182	"minutes"
VRESTORE183	"second"
VRESTORE184	"seconds"
VRESTORE185     " Block hard limit : %ld\n"
VRESTORE186     " Block soft limit : %ld\n"
VRESTORE187     " File hard limit : %ld\n"
VRESTORE188     " File soft limit : %ld\n"
VRESTORE189     " Block grace period : %s\n"
VRESTORE190     " File grace period : %s\n"
VRESTORE191     " User ID : %d\n"
VRESTORE192     " Group ID : %d\n"
VRESTORE195     " File hard limit : %d\n"
VRESTORE196     " File soft limit : %d\n"
VRESTORE197     " Block grace period : %s\n"
VRESTORE198     " File grace period : %s\n"
VRESTORE199     "%s: As requested, fileset quotas not restored.\n"
VRESTORE200     "%s: Fileset quotas in saveset differ with those in use; overwrite? "
VRESTORE201     "%s: Fileset quotas not restored.\n"
VRESTORE202     "Fileset quota data"
VRESTORE203     "User quota data"
VRESTORE204     "Group quota data"
VRESTORE205     "%s: lseek() on quota file failed: [%d] %s.\nSome quotas may not be restored.\n"
VRESTORE207     "%s: quotactl() failed: [%d] %s.\nQuotas could not be restored for %s %d.\n"
VRESTORE208     "r  %s\n"
VRESTORE209	"<data-split>"
VRESTORE213	"%s: cannot overwrite %s with %s, already exists as %s.\n"
VRESTORE214     "file"
VRESTORE215     "directory"
VRESTORE216     "device"
VRESTORE217     "symbolic link"
VRESTORE218     "unknown"
VRESTORE219	"%s: rewinding tape\n"
VRESTORE221     "%s: Need option 'f' followed by remote device \"hostname:path\"\n"
VRESTORE222     "\twhere the device is specified as hostname:device.\n"
VRESTORE223     "\n%s: unable to properly close device <%s>; [%d] %s\n"
VRESTORE224     "%s: Save-set source directory: %s\n"
VRESTORE225	"<user quotas>"
VRESTORE226	"<group quotas>"
VRESTORE227	"%s: unable to forward tape to next file; [%d] %s\n"
VRESTORE231     "%s: advfs_xtntmap_open() on quota file failed: [%d] %s.\n"
VRESTORE232     "%s: unable to reinitialize %s quotas from %d to %d.\n"
VRESTORE233     " Block hard limit : %lu\n"
VRESTORE234     " Block soft limit : %lu\n"
VRESTORE235     "\n%s: parameter for option <-f> greater than allowed. (hostname[%d]:path[%d])\n"
VRESTORE236     "\n%s: path parameter for option <-f> greater than allowed. (hostname[%d]:path[%d])\n"
VRESTORE237     "\n%s: hostname parameter for option <-f> greater than allowed. (hostname[%d]:path[%d])\n"
VRESTORE239     "%s: warning: advdump/advrestore of quotas not supported for non local filesystems.\n"
VRESTORE240     "\n%s: unable to properly close file <%s>; [%d] %s\n"
VRESTORE241	"   -Q \n"
VRESTORE242     "\tSpecifies that quota files should not be restored.\n"	
VRESTORE243	"%s: Can not decompress dump data.\n"

$set S_DIRMGT1
DIRMGT2	"%s: cannot create restore dir <%s>; [%d] %s; terminating\n"
DIRMGT3	"%s: dir_write() error; [%d] %s; terminating\n"
DIRMGT4	"%s: dir_access() lseek error; [%d] %s; terminating\n"
DIRMGT5	"%s: dir_read() seek error; [%d] %s; terminating\n"
DIRMGT6	"%s: dir_read() read error; [%d] %s; terminating\n"
DIRMGT7	"file <%s> not found; will not extract it\n"
DIRMGT8	"%s: file <%s> not found\n"


$set S_INOTBL1
INOTBL1	"%s: ino_tbl_add_dir(%d); parent ino not found\n"
INOTBL2	"%s: ino_tbl_add_dir(%d); parent not dir ino\n"
INOTBL3	"%s: out of memory; terminating\n"
INOTBL4	"%s: ino_tbl_add_file(%d); parent ino not found\n"
INOTBL5	"%s: ino_tbl_add_file(%d); parent not dir ino\n"
INOTBL6	"restore cnt is zero; cannot dec; terminating\n"


$set S_RBLKPOOL_UTIL1
RBLKPOOL_UTIL1	"%s: recovering corrupt block,"
RBLKPOOL_UTIL2	" block recovered\n"
RBLKPOOL_UTIL3	" unable to recover block\n"
RBLKPOOL_UTIL4	"Invalid block state <%d> encountered in 'rblk_get'\n"


$set S_SHELL1
SHELL1	"directory <%s> not found\n"
SHELL2	"file or directory <%s> does not exist\n"
SHELL4	"out of memory; cannot list all files\n"
SHELL6	"Options:\n"
SHELL7	"   ? | h | help \n"
SHELL8	"\tDisplay list of interactive commands. \n"
SHELL9	"   ls [<arg>] \n"
SHELL10	"\tLists all files in the save-set device or file.\n"
SHELL11	"   cd | chdir [<arg>] \n"
SHELL12	"\tChange to the directory specified in arg.\n"
SHELL13	"   add <arg> \n"
SHELL14	"\tAdd a file or directory to the set of files or \n"
SHELL15	"\tdirectories to restore. \n"
SHELL16	"   verbose | ver \n"
SHELL17	"\tSpecifies which files have been added for reading. \n"
SHELL18	"   review | rev [<arg>] \n"
SHELL19	"\tLists current directory or file specified. \n"
SHELL20	"   delete | del | rem | remove <arg> \n"
SHELL21	"\tRemove a file or directory from the set of files or\n"
SHELL22	"   pwd \n"
SHELL23	"\tPrint the current working directory in the save-set. \n"
SHELL24	"   sh <arg> \n"
SHELL25	"\tPerform system command outside of shell. \n"
SHELL26	"   extract | restore \n"
SHELL27	"\tExit the interactive shell and restore files. \n"
SHELL28	"   exit | e | quit | q \n"
SHELL29	"\tExit the interactive shell without restoring any files. \n"
SHELL30	"unknown command\n"
SHELL31	"out of memory; cannot list files\n"

$set S_PATCH1
PATCH_VRESTORE214 "%s: unable to malloc space for ACL\n"
PATCH_VRESTORE215 "%s: Unexpectedly, path does not exist for file %s, creating path without regard to protections\n"

PATCH_VRESTORE216 "%s: Unexpected termination (%d) of ACL record, skipping\n"
