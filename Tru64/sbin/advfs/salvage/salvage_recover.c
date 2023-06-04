/*
 * =======================================================================
 *   (c) Copyright Hewlett-Packard Development Company, L.P., 2008
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of version 2 the GNU General Public License as
 *   published by the Free Software Foundation.
 *   
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *   
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * =======================================================================
 *
 * Facility:
 *
 *      Advance File System
 *
 * Abstract:
 *
 *      On-disk structure salvager.
 *
 * Date:
 *
 *      Wed Feb 05 15:00:00 1997
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: salvage_recover.c,v $ $Revision: 1.1.42.2 $ (DEC) $Date: 2006/04/06 03:21:37 $"

#include "salvage.h"
#include <sys/proplist.h>

/*
 * Global Variables
 */
extern nl_catd _m_catd;
extern int num_extended_attribute_bytes;


/*
 * Function name: recover_fileset (3.7.1)
 *
 * Description:
 *  This routine sets up a pointer to the top of the fileset directory
 *  tree and calls the walk_tree routine to go down the tree and
 *  recover the files. It then does the same for the lost and found
 *  subtree.
 *
 * Input parameters:
 *  domain: pointer to domain information. 
 *  filesetp: pointer to fileset information.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */
 
int 
recover_fileset (domainInfoT *domain,
                 filesetLLT  *filesetp)                
{
    char *funcName = "recover_fileset";
    filesetTreeNodeT *nodep;    /* pointer to the top of the tree */
    recoverDataT recoverData;   /* structure needed for recover_file */
    int status;                 /* returned status for calls */
    
    /* Initialize all counters */
    
    recoverData.totalFilesFound = 0;
    recoverData.successFiles = 0;
    recoverData.partialFiles = 0;
    recoverData.unrecoveredFiles = 0;
    recoverData.noAttributeFiles = 0;
    recoverData.largerFiles = 0;
    recoverData.dirFiles = 0;
    
    /* Set up remainder of data needed to recover files */
    
    recoverData.pathName = NULL;
    recoverData.volumes = domain->volumes;
    recoverData.tagArray = filesetp->tagArray;
    recoverData.fsId = filesetp->filesetId.dirTag.num;

    /*
     * Print header message in log file with fileset being recovered
     * and fileset quotas. It doesn't make much sense to print out the
     * grace times since they are relative to the time the limit was
     * exceeded.
     */
    fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_167, 
             "\n    Recovery of fileset %s, fileset quotas:\n"), filesetp->fsName);

    if (NULL != filesetp->quota)
    {
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_168, 
                 "Hard block limit = %lu, soft block limit = %lu\n"),
                 filesetp->quota->hardBlockLimit, filesetp->quota->softBlockLimit);
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_169, 
                 "Hard file limit = %lu, soft file limit = %lu\n"),
                 filesetp->quota->hardFileLimit, filesetp->quota->softFileLimit);
    }
    
    /* Recover the files starting with the node at top of directory tree */

    nodep = filesetp->dirTreeHead;
    
    /*
     * Walk each node of the tree and recover the files using recover_file
     * or recover_file_to_tape.
     */

    switch (Options.outputFormat)
    {
	case F_DISK:
	    status = walk_tree (nodep, PARENT_FIRST, recover_file,
				&recoverData);
	    /*
	     * Before checking the status, reset the creation dates on the 
	     * directories that we've processed so far. Use the status from
	     * above to determine whether to go on or not.
	     */
	    walk_tree (nodep, CHILD_FIRST, reset_dir_dates, &recoverData);
	    break;
	case F_TAR:
	    status = walk_tree (nodep, PARENT_FIRST, recover_file_to_tape,
				&recoverData);
	    break;
	default:
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_262,
		     "Invalid output format '%d'.\n"), Options.outputFormat);
	    break;
    }

    /* If a problem occurred, write a message and statistics */
    
    if (FAILURE == status)
    {
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_170, 
                 "Could not complete recovery of fileset\n"));
        writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_171, 
                  "Could not complete recovery of fileset '%s'\n"), 
                  filesetp->fsName);
    }
    else
    {

        /* Set the node pointer to lost+found subtree */
    
        nodep = nodep->nextSibling;
    
        /* Only recover files if we have nodes under lost+found */
    
        if (nodep->children != NULL)
        {
            /* 
             * Call walk_tree with function recover_file to recover 
             * lost+found files.
             */
    
	    switch (Options.outputFormat)
	    {
		case F_DISK:
		    status = walk_tree (nodep, PARENT_FIRST, recover_file,
					&recoverData);

		    /* Reset the directory dates in the lost+found tree. */
		    walk_tree (nodep, CHILD_FIRST, reset_dir_dates,
			       &recoverData);
		    break;
		case F_TAR:
		    status = walk_tree (nodep, PARENT_FIRST,
					recover_file_to_tape, &recoverData);
		    break;
		default:
		    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1,
			     SALVAGE_262,
			     "Invalid output format '%d'.\n"),
			     Options.outputFormat);
		    break;
	    }
            
            /* If a problem occurred, write a message and statistics */
        
            if (FAILURE == status)
            {
                fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_170, 
                         "Could not complete recovery of fileset\n"));
                writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_171, 
                          "Could not complete recovery of fileset '%s'\n"),
                          filesetp->fsName);
            }
        }

    }
    if (FAILURE != status)
    {
        writemsg (SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_172, 
                  "Fileset '%s' recovery completed, check log and error files\n"), 
                  filesetp->fsName);
    }

    fprintf (Options.logFD,catgets(_m_catd, S_SALVAGE_1, SALVAGE_173, 
             "%lu files processed, including %lu directories\n"),
             recoverData.totalFilesFound, recoverData.dirFiles);
    fprintf (Options.logFD,catgets(_m_catd, S_SALVAGE_1, SALVAGE_174, 
             "%lu successfully recovered, %lu partially recovered\n"),
             recoverData.successFiles, recoverData.partialFiles);
    fprintf (Options.logFD,catgets(_m_catd, S_SALVAGE_1, SALVAGE_175, 
             "%lu recovered larger, %lu unrecovered\n"),
             recoverData.largerFiles, recoverData.unrecoveredFiles);
    fprintf (Options.logFD,catgets(_m_catd, S_SALVAGE_1, SALVAGE_176, 
             "%lu had no attributes.\n"), recoverData.noAttributeFiles);
 
    return status;
}
/* end recover_fileset */


/*
 * Function name: recover_file (3.7.2)
 *
 * Description:
 *  This routine checks if the file already exists and overwrites it
 *  or not based on command line options. If restoring the file, it
 *  restores the attributes and data based on the type of file.
 *
 * Input parameters: 
 *  nodep: pointer to tree node for this file.
 *  recoverData: pointer to data passed from recover_fileset.
 *
 * Side effect:
 *  pathName is freed.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */
 
int recover_file (filesetTreeNodeT *nodep,
                  recoverDataT     *recoverData)
{
    char *funcName = "recover_file";
    extern long maxNodes;
    filesetTagT *tagEntryp;  /* pointer to tag array entry for node */
    char *localPath;         /* pointer to path name for file creation */
    int status;              /* return status from called routines */
    char newDirPath[PATH_MAX+2];      /* new directory path */
    int stIdx = 0;     /* start index for operations on path name */
    int endIdx = PATH_MAX-1;  /* end index for operations on path name */
    int dirIdx = 0;        /* index for path name copy */
    static long fileCounter = 0;

    fileCounter++;
    if (0 != Options.progressReport) {
	if ((fileCounter % 1000) == 0) {
	    writemsg(SV_ALL | SV_CONT, 
		     catgets(_m_catd, S_SALVAGE_1, SALVAGE_284, 
			     "Recovering file %d out of %d.\r"),
		     fileCounter, maxNodes);
	}
    }

    /* Store fileset tag pointer array entry for node in tagEntryp. */
    
    tagEntryp = recoverData->tagArray[nodep->tagNum];
    
    /*
     * If the fileset tag pointer array entry is NULL, DEAD, or IGNORE there 
     * is no file to recover, so just return.
     */

    if (NULL == tagEntryp || IGNORE == tagEntryp || DEAD == tagEntryp)
    {
        return SUCCESS;
    }

    /* Increment the number of files we've processed. */
        
    recoverData->totalFilesFound++;
        
    /*
     * If the status for the tag is complete or more, or we're saving partial
     * files, then restore the file.
     */

    if ((S_IS_COMPLETE (tagEntryp->status) || S_IS_MORE (tagEntryp->status))
        || (0 != Options.recPartial))
    {

        /* Get the path for the file and store it for other routines. */
        
        status = get_pathname (&(recoverData->pathName), nodep);
        if (FAILURE == status)
        {
            return FAILURE;
        }
        localPath = recoverData->pathName;    /* Set local path to file name. */

        /* 
         * If name is too long to do a create, then chop it up here and 
         * cd to directory further down the tree, until name is less than 
         * PATH_MAX. Need to cd back to recover directory at the 
         * end of this routine.
         */
        while (strlen (localPath) >= PATH_MAX)
        { 
            /*
             * The pathname is too long, so break it up into
             * sections and change directory to each section.
             * All preceding directories should have been
             * previously created.
             */
                     
            bzero (newDirPath, sizeof (newDirPath));
            
            /* 
             * Back up from the end index until we hit a '/'. 
             * We should hit one because it's pretty hard to create
             * a path longer than PATH_MAX without getting a "name
             * too long" error along the way. However, to be paranoid,
             * we'll make sure we don't go past the current stIdx.
             */
            while ((recoverData->pathName[endIdx] != '/') && endIdx > stIdx)
            {
                endIdx--;
            }
            
            if (recoverData->pathName[endIdx] == '/')
            {
                /* Copy the new directory path and chdir to it. */
                strncpy (newDirPath, recoverData->pathName+stIdx, endIdx-stIdx);            
                if (-1 == chdir (newDirPath))
                {
                    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_24, 
                             "Can't change directory to '%s'\n"), newDirPath);
                    free (recoverData->pathName);
                    return FAILURE;
                }
            
                /*
                 * Start the local path one character past the '/' we found
                 * above and set stIdx there, too. Set the end at PATH_MAX-1 
                 * from the start and check the size of the local path again.
                 */
                localPath = recoverData->pathName + endIdx + 1;
                stIdx = endIdx + 1;
                endIdx += PATH_MAX - 1; 
            }
            else
            {
                /*
                 * If we got out of the loop without finding a '/', then
                 * just set localPath to the original name and let the 
                 * file creation error out later in the code.
                 */
                localPath = recoverData->pathName;
                chdir (Options.recoverDir);
                stIdx = 0;        /* flag that we don't need to chdir later */
                break;
                
            } /* end if we found a slash */

        } /* end while path is too long */
        /*
         * If the file doesn't have any attributes, count it here instead
         * of all the other places we check attributes.
         */
        if (NULL == tagEntryp->attrs)
        {
            recoverData->noAttributeFiles++;
        }
        
        /*
         * If we've already recovered the file, then we have found a
         * hardlink. So create the hardlink to the file.
         */

        if (S_IS_RECOVERED (tagEntryp->status))
        {
            status = restore_hardlink (nodep, recoverData->pathName, 
                                       localPath, tagEntryp);

            /* Check for failures and update appropriate file counters. */
            if (SUCCESS != status)
            {
                recoverData->unrecoveredFiles++;
                fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_177, 
                         "%s : %d : hardlink not created\n"), recoverData->pathName, 
                         nodep->tagNum);
                writemsg (SV_ALL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_178, 
                          "Hardlink '%s' not created\n"), recoverData->pathName);
                          
                if (NO_SPACE == status)
                {
                    status = FAILURE;
                }
                else
                {
                    status = SUCCESS;
                }
            }
            else
            {
                /*
                 * Count file based on tag array status
                 */
                if (S_IS_COMPLETE (tagEntryp->status) || 
                    S_IS_EMPTY (tagEntryp->status))
                {
                    recoverData->successFiles++;
                }
                else if (S_IS_PARTIAL (tagEntryp->status))
                {
                    recoverData->partialFiles++;
                }
                else if (S_IS_MORE (tagEntryp->status))
                {
                    recoverData->largerFiles++;
                }

                if (Options.verboseLog != 0)
                {
                    fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, 
                             SALVAGE_179, "%s : %d : hardlink created\n"), 
                             recoverData->pathName, nodep->tagNum);
                }
                writemsg (SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_180, 
                          "Hardlink '%s' created\n"), recoverData->pathName);
                          
            } /* end if restore_hardlink did not return SUCCESS */
            
            free (recoverData->pathName);
            
            /* If we did a chdir down into the tree, then reset to the top. */
            if (stIdx != 0)
            {
                chdir (Options.recoverDir);
            }
            return status;
        } /* end if file already recovered */

        /* Check to see if file already exists. */

        status = access (localPath, F_OK);
        if (0 == status)
        {
            if (-1 == check_overwrite (recoverData->pathName))
            {
                if (Options.verboseLog != 0)
                {
                    fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, 
                             SALVAGE_181, "%s: %d : file was not overwritten\n"),
                             recoverData->pathName, nodep->tagNum);
                }
                writemsg (SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_182, 
                          "File '%s' was not overwritten\n"), 
                          recoverData->pathName);
                recoverData->unrecoveredFiles++;
                free (recoverData->pathName);
                
                /* If we did a chdir down into the tree, then reset to the top. */
                if (stIdx != 0)
                {
                    chdir (Options.recoverDir);
                }
                return SUCCESS;
            }
            else
            {
                struct stat stat_buf; /* buffer for file stats */
                int retStatus;        /* status returned from stat call */
                int syscallStatus = 0; /* status from other syscalls */
                
                /*
                 * Find out the type of the existing file using stat.
                 */
                retStatus = stat (localPath, &stat_buf);
                 
                if (0 == retStatus)
                {
                    
                    /* 
                     * If the file isn't a directory, delete it and create a 
                     * new one later. If it is a directory, then just use the 
                     * directory without doing a removal/recreate.
                     */
                    if (S_IFDIR != (stat_buf.st_mode & S_IFMT))
                    {
                        syscallStatus = unlink (localPath);
                    }
                    else
                    {
                        /* If the existing file is a directory and the file to
                         * recover is a regular file, delete the directory and
                         * all the files under it.
                         */
                        if (FT_DIRECTORY != tagEntryp->fileType)
                        {
                             char command_str[PATH_MAX+16]; /* command string */
                             
                             sprintf (command_str, "/sbin/rm -rf %s",
                                      recoverData->pathName);
                             syscallStatus = system (command_str);
                        }
                    
                    } /* end if existing file is not a directory */
                } /* end if retStatus is 0 */
                
                if (retStatus < 0 || syscallStatus < 0)
                {
                    fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, 
                             SALVAGE_183, "%s : %d : could not be overwritten\n"),
                             recoverData->pathName, nodep->tagNum);
                    writemsg (SV_ALL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_184, 
                              "File '%s' could not be overwritten\n"),
                              recoverData->pathName);
                    recoverData->unrecoveredFiles++;
                    free (recoverData->pathName);
                    
                    /* If we did a chdir down into the tree, then reset to the top. */
                    if (stIdx != 0)
                    {
                        chdir (Options.recoverDir);
                    }
                    return SUCCESS;
                
                } /* end if stat return or syscall return less than 0 */
            } /* end if check_overwrite */
        } /* end if file already exists (0 == status) */

        /* Restore the file based on its type. */

        switch (tagEntryp->fileType)
        {
            case FT_DIRECTORY:
                status = restore_dir (tagEntryp, recoverData->pathName, localPath);
                recoverData->dirFiles++;
                break;
                
            case FT_SYMLINK:
                status = restore_symlink (tagEntryp, recoverData->pathName, 
                                          localPath);
                break;
                
            case FT_SPECIAL:
            case FT_FIFO:
                status = restore_special (nodep, recoverData->pathName,
                                          localPath, tagEntryp);
                break;
                
            /* Treat files whose type we couldn't get as regular files */
            case FT_REGULAR:
            case FT_UNKNOWN:
            default:
                /* 
                 * Pass a pointer to local path because path name can
                 * change in restore_reg_file and we need to free it
                 * later, so we need to pass an address instead of 
                 * the path name.
                 */
                status = restore_reg_file (tagEntryp, recoverData->pathName,
                                           &localPath, recoverData->volumes);
        } /* end switch on file type */

        /* If we couldn't restore the file, count it as unrecovered. */
        if (FAILURE == status)
        {
            recoverData->unrecoveredFiles++;
            fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_185, 
                     "%s : %d : could not be restored\n"), recoverData->pathName, 
                     nodep->tagNum);
            writemsg (SV_ALL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_186, 
                      "File '%s' could not be restored\n"), recoverData->pathName);
            status = SUCCESS;
        }
        else if (NO_SPACE == status)
        {
            /* 
             * If we ran out of disk space or memory return FAILURE to stop tree 
             * walk. In all other cases SUCCESS is returned to continue processing.
             */
         
            status = FAILURE;
        }
        else
        {
            /* If file was restored, count it and write message if requested. */
            if (SUCCESS == status)
            {
                /* 
                 * If the file was larger than expected, increment the 
                 * largerFiles counter, else count it as a success.
                 */
                if (S_IS_MORE (tagEntryp->status))
                {
                    recoverData->largerFiles++;
                }
                else
                {
                    /*
                     * This handles the cases of complete or empty.
                     */
                    recoverData->successFiles++;
                }
                if (Options.verboseLog != 0)
                {
                    fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, 
                             SALVAGE_187, "%s : %d : successfully restored\n"), 
                             recoverData->pathName, nodep->tagNum);
                }
                writemsg (SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_188, 
                          "File '%s' was successfully restored\n"),
                          recoverData->pathName);
                          
            } /* end if SUCCESS status */

            /* 
             * If file was partially restored, increment the counter. A
             * message was already written to the log by the called routine.
             */
            if (PARTIAL == status)
            {
                recoverData->partialFiles++;
                writemsg (SV_NORMAL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_189, 
                          "File '%s' was partially restored\n"),
                          recoverData->pathName);
                status = SUCCESS;
            }

            /* Marked the file as recovered. */
            S_SET_RECOVERED (tagEntryp->status);

            /* 
             * If this is not the first instance of the file in the tree, then
             * set the first instance pointer to this node.
             */
            if (nodep != tagEntryp->firstInstance)
            {
                tagEntryp->firstInstance = nodep;
            }

            /* 
             * If we have a property list for the file, restore it 
             */
            if (!S_IS_PL_NONE (tagEntryp->status))
            {
                status = SUCCESS;
                if (S_IS_PL_COMPLETE (tagEntryp->status))
                {
                    struct timeval fileTimes[2];  /* time values for utimes call */
                    
                    status = restore_proplist (tagEntryp, NULL,
					       recoverData->pathName,
                                               localPath);
                    /*
                     * If the attributes exist, then reset the times after
                     * restoring the property list, since restoring the
                     * property list changes them.
                     */
                    if (NULL != tagEntryp->attrs)
                    {
                        /*
                         * Copy atime and mtime from attributes into fileTimes 
                         * for utimes call.
                         */
                        fileTimes[0] = tagEntryp->attrs->atime;
                        fileTimes[1] = tagEntryp->attrs->mtime;
    
                        if (-1 == utimes (localPath, fileTimes))
                        {
                            fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, 
                                 SALVAGE_211, 
                                 "%s: can't restore file atime/mtime\n"), 
                                 recoverData->pathName);
                        }
                    }
                }
                
                if (S_IS_PL_PARTIAL (tagEntryp->status) || (SUCCESS != status))
                {
                    if (NO_SPACE == status)
                    {
                        status = FAILURE;
                    }
		    else
		    {
                        status = SUCCESS;
		    }
                    fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, 
                             SALVAGE_190, 
                             "%s : %d : property list not restored\n"),
                             recoverData->pathName, nodep->tagNum);
                    writemsg (SV_ALL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_191, 
                              "Property list not restored for file '%s'\n"),
                              recoverData->pathName);
                } /* end if partial property list */
            } /* end if property list exists */
        } /* end if FAILURE status */

        /*
         * If we created a partial file and had to allocate a new
         * string for the local path, then we need to free it. If we
         * didn't allocate a new local path, then localPath should lie
         * between recoverData->pathName and it's end.
         */        
        if (S_IS_PARTIAL (tagEntryp->status) && 
            ((localPath < recoverData->pathName) ||
             (localPath > (recoverData->pathName
                             + strlen (recoverData->pathName)))))
        {
            free (localPath);
        }
        free (recoverData->pathName);
    }
    else
    {
        /*
         * Count the file as unrecovered if we didn't process it.
         */
        recoverData->unrecoveredFiles++;
        status = SUCCESS;

    } /* end if file status is complete or more or restore partial is set */

    /* If we did a chdir down into the tree, then reset to the top. */
    if (stIdx != 0)
    {
        chdir (Options.recoverDir);
    }
     
    return status;
}
/* end recover_file */


/*
 * Function name: recover_file_to_tape (3.7.x)
 *
 * Description:
 *  This routine saves a file's attributes and data to a tape archive.
 *  It is analogous to recover_file(), performing similar functionality
 *  except that it will output files to a tape archive.
 *
 * Input parameters: 
 *  nodep: pointer to tree node for this file.
 *  recoverData: pointer to data passed from recover_fileset.
 *
 * Side effect:
 *  pathName is freed.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */
 
int recover_file_to_tape (filesetTreeNodeT *nodep,
			  recoverDataT     *recoverData)
{
    char *funcName = "recover_file_to_tape";
    extern long maxNodes;
    filesetTagT *tagEntryp;  /* pointer to tag array entry for node */
    Stat statbuf;	     /* buffer to store stat structure for this file */
    int status;              /* return status from called routines */
    int align;		     /* padding alignment returned from writetar */
    char *partialName;       /* for when ".partial" needs to be added */
    static char pathNameBuffer[MAXPATHLEN + 1];
    char *tarPathName;
    static long fileCounter = 0;

    fileCounter++;
    if (0 != Options.progressReport) {
	if ((fileCounter % 1000) == 0) {
	    writemsg(SV_ALL | SV_CONT, 
		     catgets(_m_catd, S_SALVAGE_1, SALVAGE_284, 
			     "Recovering file %d out of %d.\r"),
		     fileCounter, maxNodes);
	}
    }

    if (Options.outputFormat != F_TAR)
    {
	writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_262,
		 "Invalid output format '%d'.\n"), Options.outputFormat);
	return FAILURE;
    }

    /* Store fileset tag pointer array entry for node in tagEntryp. */
    
    tagEntryp = recoverData->tagArray[nodep->tagNum];
    
    /*
     * If the fileset tag pointer array entry is NULL, DEAD, or IGNORE there 
     * is no file to recover, so just return.
     */

    if (NULL == tagEntryp || IGNORE == tagEntryp || DEAD == tagEntryp)
    {
        return SUCCESS;
    }

    /* Increment the number of files we've processed. */
        
    recoverData->totalFilesFound++;
        
    /*
     * If the status for the tag is complete or more, or we're saving partial
     * files, then restore the file.
     */

    if ((S_IS_COMPLETE (tagEntryp->status) || S_IS_MORE (tagEntryp->status))
        || (0 != Options.recPartial))
    {

        /* Get the path for the file and store it for other routines. */
        
        status = get_pathname (&(recoverData->pathName), nodep);
        if (FAILURE == status)
	{
            return FAILURE;
        }

	/*
	 * Append ".partial" to filename if it's partial and requested
	 */
	if ((S_IS_PARTIAL (tagEntryp->status)) && (0 != Options.addSuffix))
	{
	    partialName = salvage_malloc (strlen (recoverData->pathName) + 9);
	    if (NULL == partialName)
	    {
		writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_198, 
			  "Out of memory processing file '%s'\n"),
			  recoverData->pathName);
                free (recoverData->pathName);
		return FAILURE;
	    }
	    
	    /*
	     * Copy the filename and add .partial to the end.
	     */
         
	    strncpy (partialName, recoverData->pathName,
		     strlen(recoverData->pathName) + 1);
	    strncat (partialName, catgets(_m_catd, S_SALVAGE_1, SALVAGE_199, 
					  ".partial"), 9);
	    
	    /*
	     * Free pathName and substitute partialName for it.
	     */
	    free (recoverData->pathName);
	    recoverData->pathName = partialName;
	}

	if (strlen(recoverData->pathName) > PATH_MAX - 1)
	{
	    recoverData->unrecoveredFiles++;
	    fprintf(Options.logFD, catgets(_m_catd, S_SALVAGE_1,
		    SALVAGE_288, "%s : %d : path too long to restore to tar archive\n"),
		    recoverData->pathName, nodep->tagNum);
	    writemsg(SV_ALL, catgets(_m_catd, S_SALVAGE_1,
		     SALVAGE_289, "File '%s' has a path too long to restore to tar archive\n"),
		     recoverData->pathName);
            free (recoverData->pathName);
	    return SUCCESS;
	}

	bzero(pathNameBuffer, PATH_MAX);
	strcpy(pathNameBuffer, recoverData->pathName);
	tarPathName = pathNameBuffer;

	/*
	 * If the file doesn't have any attributes, count it here
	 * instead of all the other places we check attributes.
	 */
	if (NULL == tagEntryp->attrs)
	{
	    recoverData->noAttributeFiles++;
	    
	    fprintf(Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_204, 
		    "%s: %d : attributes were not found\n"),
		    recoverData->pathName, nodep->tagNum);
	    writemsg(SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_205, 
		     "Attributes not found for file '%s'\n"),
		     recoverData->pathName);

	    /* Initialize the rest of the stat buffer to a default. */
	    statbuf.sb_mode = S_IFREG | S_IRWXU;
	    statbuf.sb_uid = 0;
	    statbuf.sb_gid = 0;
	    statbuf.sb_size = 0;
	    statbuf.sb_atime = 0;
	    statbuf.sb_mtime = 0;
	    statbuf.sb_ctime = 0;
	}
	else
	{
	    statbuf.sb_mode = tagEntryp->attrs->mode;
	    statbuf.sb_uid = tagEntryp->attrs->uid;
	    statbuf.sb_gid = tagEntryp->attrs->gid;
	    statbuf.sb_size = tagEntryp->attrs->size;
	    statbuf.sb_atime = tagEntryp->attrs->atime.tv_sec;
	    statbuf.sb_mtime = tagEntryp->attrs->mtime.tv_sec;
	    statbuf.sb_ctime = tagEntryp->attrs->ctime.tv_sec;
	}
	
	/*
	 * If the algorithm to calculate size changes, make sure to change it
	 * in restore_file_data() also.
	 */
	
	if (0 == tagEntryp->bytesFound)
	{
	    statbuf.sb_size = 0;
	}
	else
	{
	    statbuf.sb_size = MAX(statbuf.sb_size,
				  tagEntryp->bytesFound);
	}
	
	statbuf.sb_dev = recoverData->fsId;
	statbuf.sb_ino = nodep->tagNum;
	statbuf.sb_nlink = tagEntryp->linksFound;

	/*
	 * Clear file type mode bits ; each fileType sets these
	 * bits appropriately, except for block and char devices.
	 */
	if (tagEntryp->fileType != FT_SPECIAL)
	{
	    statbuf.sb_mode = statbuf.sb_mode & ~S_IFMT;
	}

	switch (tagEntryp->fileType)
	{
	    case FT_DIRECTORY:
		statbuf.sb_mode |= S_IFDIR;
		statbuf.sb_nlink = 1;
		statbuf.sb_size = 0;
		break;
	    case FT_SPECIAL:
		/* 
		 * statbuf.sb_mode is the only place BLK vs. CHR is
		 * determined, so leave it alone
		 */
		statbuf.sb_size = 0;
		if (NULL == tagEntryp->addAttrs)
		{
		    recoverData->unrecoveredFiles++;
		    fprintf(Options.logFD, catgets(_m_catd, S_SALVAGE_1,
			    SALVAGE_185, "%s : %d : could not be restored\n"),
			    recoverData->pathName, nodep->tagNum);
		    writemsg(SV_ALL, catgets(_m_catd, S_SALVAGE_1,
			     SALVAGE_186, "File '%s' could not be restored\n"),
			     recoverData->pathName);
                    free (recoverData->pathName);
		    return SUCCESS;
		}
		else
		{
		    statbuf.sb_rdev = *(dev_t *)(tagEntryp->addAttrs);
		}
		break;
	    case FT_FIFO:
		statbuf.sb_mode |= S_IFIFO;
		statbuf.sb_size = 0;
		break;
	    case FT_SYMLINK:
		statbuf.sb_mode |= S_IFLNK;
		if (NULL == tagEntryp->addAttrs)
		{
		    fprintf(Options.logFD, catgets(_m_catd, S_SALVAGE_1,
			    SALVAGE_214, 
			    "%s: %d : symlink has no name to link to\n"),
			    recoverData->pathName, nodep->tagNum);
		    statbuf.sb_link[0] = '\0';
		    statbuf.sb_size = 0;
		}
		else
		{
		    strncpy(statbuf.sb_link, tagEntryp->addAttrs,
			    PATH_MAX);
		    statbuf.sb_size = strlen(statbuf.sb_link);
		}
		break;
	    case FT_REGULAR:
	    case FT_DEFAULT:
	    case FT_UNKNOWN:
	    default:
		statbuf.sb_mode |= S_IFREG;
		break;
	}
	
	/* 
	 * If we have a property list for the file, restore it 
	 */
	if (!S_IS_PL_NONE (tagEntryp->status))
	{
	    status = SUCCESS;
	    if (S_IS_PL_COMPLETE (tagEntryp->status))
	    {
		file_has_property_list = TRUE;
		status = restore_proplist (tagEntryp,
					   &statbuf,
					   recoverData->pathName,
					   tarPathName);
		file_has_property_list = FALSE;
	    }
	    
	    if (S_IS_PL_PARTIAL (tagEntryp->status) ||
		(SUCCESS != status))
	    {
		if (NO_SPACE == status)
		{
		    status = FAILURE;
		}
		else
		{
		    status = SUCCESS;
		}
		fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1,
			 SALVAGE_190, 
			 "%s : %d : property list not restored\n"),
			 recoverData->pathName, nodep->tagNum);
		writemsg (SV_ALL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_191, 
			  "Property list not restored for file '%s'\n"),
			  recoverData->pathName);
	    } /* end if partial property list */
	} /* end if property list exists */

	/*
	 * If linksFound is greater than 1, this is a hardlink.
	 * Check if we've saved this file or not, and then either
	 * save it or save a link to the original.
	 */
		
	if (tagEntryp->linksFound > 1)
	{
	    if (NULL != islink(recoverData->pathName, &statbuf))
	    {
		statbuf.sb_size = 0;
	    }
	    linkto(recoverData->pathName, &statbuf);
	}
	
	/* Save the file to tape. */
	
	status = SUCCESS;
	align = writetar(tarPathName, &statbuf);
	if (align != -1)
	{ 
	    have_archive_data = TRUE;
	    if (statbuf.sb_size > 0)
	    {
		status = restore_file_data(0, tagEntryp, recoverData->volumes);
		/* status is checked ~10 lines down from here */
	    }
	    if (align != 0)
	    {
		buf_pad((OFFSET) 1);
	    }
	}
	
	if (tagEntryp->fileType == FT_DIRECTORY)
	{
	    recoverData->dirFiles++;
	}
	
	if (SUCCESS == status &&
	    (tagEntryp->fileType == FT_REGULAR ||
	     tagEntryp->fileType == FT_DEFAULT ||
	     tagEntryp->fileType == FT_UNKNOWN)   &&
	    S_IS_PARTIAL(tagEntryp->status))
	{
	    extentLLT *extentp;    /* pointer to extent list */
		
	    /* Write beginning of partial file message to log */
        
	    fprintf(Options.logFD, catgets(_m_catd, S_SALVAGE_1,
		    SALVAGE_206, "%s: %d : partially recovered\n"),
		    recoverData->pathName, nodep->tagNum);
                 
	    /* 
	     * Loop through extents and print out the byte ranges that
	     * could not be restored (i.e. block = -2)
	     */
	    extentp = tagEntryp->extentHead;
		
	    while (NULL != extentp)
	    {
		if ( CORRUPT_RECORD == extentp->diskBlock)
		{
		    fprintf(Options.logFD, catgets(_m_catd, S_SALVAGE_1,
			    SALVAGE_207, "      missing bytes %lu to %lu\n"),
			    extentp->byteInFile, 
			    (extentp->byteInFile + extentp->extentSize - 1));
		}
		extentp = extentp->next;
	    }
	    status = PARTIAL;
	}

	/* If we couldn't restore the file, count it as unrecovered. */
	if (FAILURE == status)
	{
	    recoverData->unrecoveredFiles++;
	    fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_185, 
		     "%s : %d : could not be restored\n"),
		     recoverData->pathName, nodep->tagNum);
	    writemsg (SV_ALL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_186, 
		      "File '%s' could not be restored\n"),
		      recoverData->pathName);
	    status = SUCCESS;
	}
	else if (NO_SPACE == status)
	{
	    /* 
	     * If we ran out of disk space or memory return
	     * FAILURE to stop tree walk. In all other cases
	     * SUCCESS is returned to continue processing.  
	     */
	    
	    status = FAILURE;
	}
	else
	{
	    /*
	     * If any of the file was restored, count it and write message
	     * if requested.
	     */
	    if (SUCCESS == status)
	    {
		/* 
		 * If the file was larger than expected, increment the 
		 * largerFiles counter, else count it as a success.
		 */
		if (S_IS_MORE (tagEntryp->status))
		{
		    recoverData->largerFiles++;
		}
		else
		{
		    /*
		     * This handles the cases of complete or empty.
		     */
		    recoverData->successFiles++;
		}
		if (Options.verboseLog != 0)
		{
		    fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1,
			     SALVAGE_187,
			     "%s : %d : successfully restored\n"), 
			     recoverData->pathName, nodep->tagNum);
		}
		writemsg (SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1,
		          SALVAGE_188, 
			  "File '%s' was successfully restored\n"),
			  recoverData->pathName);
		
	    } /* end if SUCCESS status */
		    
	    /* 
	     * If file was partially restored, increment the counter.
	     */
	    if (PARTIAL == status)
	    {
		recoverData->partialFiles++;
		writemsg (SV_NORMAL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_189,
			  "File '%s' was partially restored\n"),
			  recoverData->pathName);
		status = SUCCESS;
	    }
		    
	    /* Marked the file as recovered. */
	    S_SET_RECOVERED (tagEntryp->status);
		    
	    /* 
	     * If this is not the first instance of the file
	     * in the tree, then set the first instance pointer
	     * to this node.
	     */
	    if (nodep != tagEntryp->firstInstance)
	    {
		tagEntryp->firstInstance = nodep;
	    }
	}
	
	free (recoverData->pathName);
    }
    else
    {
        /*
         * Count the file as unrecovered if we didn't process it.
         */
        recoverData->unrecoveredFiles++;
        status = SUCCESS;

    } /* end if file status is complete or more or restore partial is set */
     
    return status;
}
/* end recover_file_to_tape */


/*
 * Function name: restore_dir (3.7.3)
 *
 * Description:
 *
 * This routine restores a directory if it doesn't exist and sets the 
 * directory attributes.
 *
 * Input parameters: 
 *  tagEntryp: pointer to tag pointer array entry for this file.
 *  pathName: pointer for full pathname of file being restored.
 *  localPath: pointer to the local pathname of the file being restored.
 *
 * Returns:
 *  Status value - SUCCESS, NO_SPACE, or FAILURE. 
 */

int restore_dir (filesetTagT *tagEntryp,
                 char        *pathName,
                 char        *localPath)
{
    char *funcName = "restore_dir";
    int retry;             /* retry flag for out of space failure */
    int status;            /* status to return to caller */
    mode_t mode = S_IRWXU; /* mode for mkdir call */
    int tagNum;            /* tag number for log messages */
    
    tagNum = tagEntryp->firstInstance->tagNum;
    status = access (localPath, F_OK);
    
    /*
     * If the directory doesn't exist, then create it.
     */
    if (status != 0)
    {
        do
        {
            retry = 0;
            errno = 0;
            
            /*
             * Create the directory with owner privs only for now.
             * It will change later when attributes are restored.
             */

            status = mkdir (localPath, mode);
            if (0 != status)
            {
                /*
                 * If the error we got was lack of disk space, then see
                 * if the user wants to continue after they create some
                 * space for us.
                 */
                if (ENOSPC == errno)
                {
                    writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_192, 
                              "No room to create file '%s'\n"), pathName);
                    writemsg (SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_193, 
                             "Add more space to fileset and continue or abort\n"));
                    if (want_abort ())
                    {
                        return NO_SPACE;
                    }
                    else
                    {
                        retry++;
                    } /* end if want abort */
                }
                else
                {
                    return FAILURE;
                } /* end if errno is ENOSPC */
            } /* end if mkdir status is not 0 */
        }
        while (0 != retry);
    } /* end if access status is not 0 */

    /*
     * At this point, either the directory previously existed or we just
     * created it. In any case, change the attributes.
     */
    if (NULL != tagEntryp->attrs)
    {
        if (SUCCESS != restore_attrs (pathName, localPath, tagEntryp->attrs))
        {
            fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_194, 
                     "%s: %d : Error setting file attributes\n"), 
                     pathName, tagNum);
            writemsg (SV_NORMAL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_195, 
                      "Error setting attributes for file '%s'\n"), pathName);
        }
    }
    else
    {
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_196, 
                 "%s: %d : No attributes found for directory\n"), 
                 pathName, tagNum);
        writemsg (SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_197, 
                  "No attributes found for file '%s'\n"), pathName);
    }
    return SUCCESS;
}
/* end restore_dir */


/*
 * Function name: restore_reg_file (3.7.4)
 *
 * Description:
 *
 * This routine restores a regular file. If an error occurs while reading the 
 * data, the file is now partial and is either restores that way or deleted if 
 * we are not restoring partial files.
 *
 * Input parameters: 
 *  tagEntryp: pointer to tag pointer array entry for this file.
 *  pathName: pointer for pathname of file being restored.
 *  localPathp: pointer to address of local pathname of file being restored,
 *        this needs to be freed in the caller if we add .partial to the name.
 *  volumes: pointer to volume information.
 *
 * Returns:
 *  Status value - SUCCESS, PARTIAL, NO_SPACE, or FAILURE. 
 */

int restore_reg_file (filesetTagT *tagEntryp,
                      char        *pathName,
                      char        **localPathp,
                      volMapT     *volumes)
{
    char *funcName = "restore_reg_file";
    int fd;                /* file descriptor for file being restored */
    int retry;             /* retry flag for out of space failure */
    int status;            /* returned status from called routines */
    mode_t mode = S_IRWXU; /* mode for open call */
    char *partialName;     /* pointer to file name + .partial if needed */
    int tagNum;            /* tag number for log messages */

    tagNum = tagEntryp->firstInstance->tagNum;    
    
    /*
     * Open the file for creation and check if there is
     * enough disk space to create it.
     */
    if ((S_IS_PARTIAL (tagEntryp->status)) && (0 != Options.addSuffix))
    {
        /*
         * If the path will be too long after adding ".partial", then
         * print a message and just use the normal name.
         */
        if (strlen (*localPathp) + 9 >= PATH_MAX)
        {
            writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_267,
            "Path too long for file %s to add '.partial', using original name.\n"),
             pathName);
        }
        else
        {
            partialName = salvage_malloc (strlen (*localPathp) + 9);
            if (NULL == partialName)
            {
                writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_198, 
                          "Out of memory processing file '%s'\n"), pathName);
                return NO_SPACE;
            }
         
            /*
             * Copy the filename and add .partial to the end.
             */
         
            strncpy (partialName, *localPathp, strlen(*localPathp) + 1);
            strncat (partialName, catgets(_m_catd, S_SALVAGE_1, SALVAGE_199, 
                     ".partial"), 9);
         
            /*
             * Substitute partialName for localPath.
             */
            *localPathp = partialName;
        }
    }
    
    do
    {
        retry = 0;
        fd = open (*localPathp, O_CREAT | O_RDWR, mode);
        
        /*
         * Check to see if we got an error on the open.
         */
        if (-1 == fd)
        {
            /*
             * If the error we got was lack of disk space, then see
             * if the user wants to continue after they create some
             * space for us.
             */
            if (ENOSPC == errno)
            {
                writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_192, 
                          "No room to create file '%s'\n"), pathName);
                writemsg (SV_ERR | SV_CONT, 
                          catgets(_m_catd, S_SALVAGE_1, SALVAGE_193, 
                          "Add more space to fileset and continue or abort\n"));
                if (want_abort ())
                {
                    return NO_SPACE;
                }
                else
                {
                    retry++;
                } /* end if want abort */
            }
            else
            {
                return FAILURE;
            } /* end if errno is ENOSPC */
        } /* end if fd is -1 */
    }
    while (0 != retry);

    /*
     * Try to recover the file data only if we actually found something on disk.
     */
    if (0 <= tagEntryp->bytesFound)
    {
        status = restore_file_data (fd, tagEntryp, volumes);
        close (fd);
        
        /*
         * If we ran out of disk space while writing, return NO_SPACE
         * to stop further file processing.
         */
        if (NO_SPACE == status)
        {
            return NO_SPACE;
        }
        
        /*
         * If the file was marked partial or if a read error occurred 
         * causing a partial file to be restored when we thought we had all 
         * the data, delete the file if we aren't restoring partial files.
         */
        if (PARTIAL == status)
        {
            if (0 == Options.recPartial)
            {
                unlink (*localPathp);
            }
            else
            {
                /*
                 * If we are restoring partial files, then rename this one
                 * with the .partial suffix if that option was selected.
                 */
                if ((0 != Options.addSuffix) && 
                    (NULL == strstr (*localPathp, catgets(_m_catd, S_SALVAGE_1, 
                                     SALVAGE_199, ".partial"))))
                {
                
                    /*
                     * If the path will be too long after adding ".partial",
                     * then print a message and just use the normal name.
                     */
                    if (strlen (*localPathp) + 9 >= PATH_MAX)
                    {
                        writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_267,
                        "Path too long for file %s to add '.partial', using original name.\n"),
                         pathName);
                    }
                    else
                    {
                        /* 
                         * Create the new name with the .partial suffix.
                         */
                        partialName = salvage_malloc (strlen (*localPathp) + 9);
                        if (NULL == partialName)
                        {
                            writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_198, 
                                  "Out of memory processing file '%s'\n"), pathName);
                            return NO_SPACE;
                        }
         
                        /*
                         * Copy the filename and add .partial to the end.
                         */
         
                        strncpy (partialName, *localPathp , strlen (*localPathp));
                        strncat (partialName, catgets(_m_catd, S_SALVAGE_1, SALVAGE_199, 
                                 ".partial"), 9);
         
                        do
                        {
                            retry = 0;
                        
                            /*
                             * Rename the file using the name w/ .partial suffix.
                             */
                            errno = 0;
                            status = rename (*localPathp, partialName);
                        
                            if (-1 == status)
                            {
                                /*
                                 * If the error we got was lack of disk space, then see
                                 * if the user wants to continue after they create some
                                 * space for us.
                                 */
                                if (ENOSPC == errno)
                                {
                                    writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, 
                                              SALVAGE_200, 
                                              "No room to rename file '%s'\n"), 
                                              pathName);
                                    writemsg (SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1,
                                      SALVAGE_193, 
                                      "Add more space to fileset and continue or abort\n"));
                                    if (want_abort ())
                                    {
                                        free (partialName);
                                        return NO_SPACE;
                                    }
                                    else
                                    {
                                        retry++;
                                    } /* end if want abort */
                                }
                                else
                                {
                                    free (partialName);
                                    return FAILURE;
                                } /* end if errno is ENOSPC */
                            } /* end if rename status is -1 */
                        }
                        while (0 != retry);
                    
                        /*
                         * Substitute partialName for localPath.
                         */
                        *localPathp = partialName;
                    } /* if path too long to add .partial */                   
                } /* if adding .partial suffix */
            } /* if recovering partial files */
        }
        else
        {
            if (S_IS_STRIPED (tagEntryp->status))
            {
                fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_201, 
                         "%s : %d : was originally striped and will not be when restored\n"),
                         pathName, tagNum);
                writemsg (SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_202, 
                          "File '%s' was originally striped and is recovered unstriped\n"),
                          pathName);
            }
        } /* end if PARTIAL status */
    }
    else
    {
        close (fd);
    } /* end if bytesFound > 0 */
    
    if (NULL != tagEntryp->attrs)
    {
        if (SUCCESS != (status = restore_attrs (pathName, *localPathp, 
                                                tagEntryp->attrs)))
        {
            fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_203, 
                     "%s : %d : Error setting file attributes\n"), 
                     pathName, tagNum);
            writemsg (SV_NORMAL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_195, 
                     "Error setting attributes for file '%s'\n"), pathName);
        }
    }
    else
    {
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_204, 
                 "%s: %d : attributes were not found\n"), pathName, tagNum);
        writemsg (SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_205, 
                  "Attributes not found for file '%s'\n"), pathName);
    }
    
    if (S_IS_PARTIAL (tagEntryp->status))
    {
        extentLLT *extentp;    /* pointer to extent list */
        
        /* Write beginning of partial file message to log */
        
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_206, 
                 "%s: %d : partially recovered\n"), pathName, tagNum);
                 
        /* 
         * Loop through extents and print out the byte ranges that
         * could not be restored (i.e. block = -2)
         */
        extentp = tagEntryp->extentHead;
        
        while (NULL != extentp)
        {
            if (CORRUPT_RECORD == extentp->diskBlock)
            {
                fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_207, 
                         "      missing bytes %lu to %lu\n"), extentp->byteInFile, 
                         (extentp->byteInFile + extentp->extentSize -1));
            }
            extentp = extentp->next;
        }
        return PARTIAL;
    }
    return SUCCESS;
}
/* end restore_reg_file */


/*
 * Function name: restore_file_data (3.7.5)
 *
 * Description:
 *  This routine reads the extents for a file to obtain the disk blocks
 *  which contain the file data.  It then reads the file data and writes
 *  it to the output file. This routine assumes the extents have been sorted
 *  by byte in file.
 *
 * Input parameters: 
 *  fd: file descriptor for file being restored.
 *  tagEntryp: pointer to tag pointer array entry for this file.
 *  volumes: pointer to volume information
 *
 * Returns:
 *  Status value - SUCCESS, PARTIAL, or NO_SPACE. 
 */
int restore_file_data (int         fd,
                       filesetTagT *tagEntryp,
                       volMapT     *volumes)
{
    char *funcName = "restore_file_data";
    int vfd;             /* file descriptor for volume */
    extentLLT *extentp;  /* pointer to extent list for file */
    extentLLT *prevExtp; /* pointer to previous extent in list */
    extentLLT *newExtp;  /* pointer used to create new extents if needed */
    long bytePosition;   /* current byte position in file */
    long extSize;        /* current extent size */
    LBNT extLBN;         /* current LBN for extent */
    long readLength;     /* number of bytes for read operation */
    int status = SUCCESS; /* status returned from calls */
    int returnstatus = SUCCESS;   /* return status for this routine */
    int rwstatus = SUCCESS;        /* status from read or write */
    int retry;           /* retry flag for out of space failure */
    char buffer[PAGESIZE];   /* buffer for reading in data from disk */
    OFFSET pad;		 /* Amount of padding needed for tar output buffer */
    char *c;		 /* Temporary character pointer */
    long tmpByteInFile;
    long bytesWritten = 0;
    long tarFileSize;

    if (Options.outputFormat == F_TAR)
    {
	/*
	 * Re-calculate the size written to the tar header.
	 * If this algorithm changes, make sure to change it
	 * in restore_file_to_tape() also.
	 */

	if (NULL == tagEntryp->attrs)
	{
	    tarFileSize = 0;
	}
	else
	{
	    tarFileSize = tagEntryp->attrs->size;
	}

	if (0 == tagEntryp->bytesFound)
	{
	    tarFileSize = 0;
	}
	else
	{
	    tarFileSize = MAX(tarFileSize, tagEntryp->bytesFound);
	}
    }

    /*
     * Point to the first extent for the file.
     */
    extentp = tagEntryp->extentHead;
    prevExtp = NULL;
    
    /*
     * Loop through extents till end of list
     */
    while (NULL != extentp)
    {
        /*
         * Get the byte position in the file, size, and LBN for this extent.
         */
        bytePosition = extentp->byteInFile;
        extSize = extentp->extentSize;
        extLBN = extentp->diskBlock;
        
        /*
         * Skip over any extents which represent holes in the file - either
         * from sparse files or missing extents. Holes have LBN and volume
         * set to either -1 (known sparse file) or -2 (hole found by salvage
         * due to corrupt metadata or error reading disk).
         */
        while (extLBN == XTNT_TERM || extLBN ==CORRUPT_RECORD)
        {
	    switch (Options.outputFormat)
	    {
		case F_DISK:
		    /*
		     * Lseek past the hole and set up for the next extent,
		     * if there is one.
		     */
		    lseek (fd, (off_t) bytePosition + extSize, SEEK_SET);
		    break;
		case F_TAR:
		    status = output_tar_data(tarFileSize, extSize,
					     TRUE, NULL, &bytesWritten);
		    if (status != SUCCESS)
		    {
			S_SET_PARTIAL (tagEntryp->status);
			returnstatus = PARTIAL;
			goto _return;
		    }
		    break;
		default:
		    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1,
		       SALVAGE_262,
		       "Invalid output format '%d'.\n"), Options.outputFormat);
		    break;
	    }

            prevExtp = extentp;
            extentp = extentp->next;
            
            /*
             * If no next extent, then return.
             */
            if (NULL == extentp)
            {
		goto _return;
            }
            
            /*
             * Set up variables for next extent
             */
            bytePosition = extentp->byteInFile;
            extSize = extentp->extentSize;
            extLBN = extentp->diskBlock;
        }
        /*
         * Get the volume for this extent, read the extent from disk (page
         * by page), and write it to the file.
         */
        vfd = (&volumes[extentp->volume])->volFd;
        while (extSize > 0)
        {
	    long bytesRead;

            if (extSize < PAGESIZE)
            {
                readLength = extSize;
                status = read_bytes_by_lbn (vfd, readLength, buffer, 
					    extLBN, &bytesRead);
            }
            else
            {
                readLength = PAGESIZE;
                status = read_page_by_lbn (vfd, buffer, extLBN, &bytesRead);
            }
            if ((SUCCESS != status) || (readLength != bytesRead))
            {
                rwstatus = FAILURE;
            }
            else
            {
		switch (Options.outputFormat)
		{
		    case F_DISK:
			do
			{
			    retry = 0;

			    /*
			     * Write the data to the file.
			     */
			    if (-1 == write (fd, buffer, readLength))
			    {
				/*
				 * If the error we got was lack of disk space,
				 * then see if the user wants to continue after
				 * they create some space for us.
				 */
				if (ENOSPC == errno)
				{
				    writemsg (SV_ERR, catgets(_m_catd,
					      S_SALVAGE_1, SALVAGE_208, 
					      "No more disk space for file recovery\n"));

				    writemsg (SV_ERR | SV_CONT, catgets(_m_catd,
					      S_SALVAGE_1, SALVAGE_193, 
					      "Add more space to fileset and continue or abort\n"));

				    if (want_abort ())
				    {
					returnstatus = NO_SPACE;
					goto _return;
				    }
				    else
				    {
					retry++;
				    } /* end if want abort */
				}
				else
				{
				    returnstatus = FAILURE;
				    goto _return;
				} /* end if errno is ENOSPC */
			    } /* end if write to file failed */
			}
			while (0 != retry);
			break;
		    case F_TAR:
			status = output_tar_data(tarFileSize, readLength,
						 FALSE, buffer, &bytesWritten);
			if (status != SUCCESS)
			{
			    S_SET_PARTIAL (tagEntryp->status);
			    returnstatus = PARTIAL;
			    goto _return;
			}
			break;
		    default:
			writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1,
				 SALVAGE_262,
				 "Invalid output format '%d'.\n"),
				 Options.outputFormat);
			break;
		}

            } /* end if status not SUCCESS */
            
            /*
             * If we had a failure on the read or write, treat it as a missing
             * portion of the file.
             */
            if (FAILURE == rwstatus)
            {
                rwstatus = SUCCESS;

                /*
                 * If we aren't restoring partial files, don't bother getting 
                 * the rest of the file since it will be deleted by the caller.
                 */
                if (0 == Options.recPartial)
                {
		    /*
		     * If this is a TAR restore, _return will pad the file
		     * with zeroes.
		     */
		    returnstatus = PARTIAL;
		    goto _return;
                }
                else
                {
                    /*
                     * Set the file status to partial and create a new extent 
                     * for the page with the I/O error.
                     */
                    S_SET_PARTIAL (tagEntryp->status);
                    returnstatus = PARTIAL;
                    
		    if (Options.outputFormat == F_TAR)
		    {
			tmpByteInFile = extentp->byteInFile;
		    }

                    /*
                     * If there is only one page in this extent, then just
                     * set the LBN to -2.
                     */
                    if (extentp->extentSize == readLength)
                    {
                        extentp->diskBlock = CORRUPT_RECORD;
                    }
                    
                    /* 
                     * If at the beginning of the extent, then create the
                     * new extent before this one or add it to a previous
                     * hole.
                     */
                    else if (extLBN == extentp->diskBlock)
                    {
                        extentp->extentSize -= readLength; /* decrement size */
                        extentp->diskBlock += readLength/512;
                        /*
                         * If the previous extent is just a hole,
                         * then just add the size of data read to it
                         * and subtract it from this extent.
                         */
                        if (NULL != prevExtp && PERM_HOLE_START == prevExtp->diskBlock)
                        {
                            prevExtp->extentSize += readLength;
                            extentp->byteInFile += readLength;
                        }
                        else
                        {
                            /*
                             * Create a new extent in front of this one.
                             */
                            newExtp = NULL;
                            status = create_extent_element (&newExtp, -2, (LBNT)-2,
                                     readLength, extentp->byteInFile);
                            if (NO_MEMORY == status)
                            {
                                returnstatus = NO_SPACE;
				goto _return;
                            }
                            /*
                             * Insert the new extent between the previous
                             * extent and this one. If the previous is NULL,
                             * then put the new one at the head of the list.
                             */
                            newExtp->next = extentp;
                            if (NULL != prevExtp)
                            {
                                prevExtp->next = newExtp;
                            }
                            else
                            {
                                tagEntryp->extentHead = newExtp;
                            }
                            prevExtp = newExtp;
                            extentp->byteInFile += readLength;
                            
                            /* Lseek past the hole in the file. */
			    switch (Options.outputFormat)
			    {
				case F_DISK:
				    lseek (fd, (off_t) extentp->byteInFile,
					   SEEK_SET);
				    break;
				case F_TAR:
				    status = output_tar_data(tarFileSize,
						     extentp->byteInFile -
						     tmpByteInFile,
						     TRUE, NULL,
						     &bytesWritten);
				    if (status != SUCCESS)
				    {
					S_SET_PARTIAL (tagEntryp->status);
					returnstatus = PARTIAL;
					goto _return;
				    }
				    break;
				default:
				    writemsg(SV_DEBUG, catgets(_m_catd,
					     S_SALVAGE_1, SALVAGE_262,
					     "Invalid output format '%d'.\n"),
					     Options.outputFormat);
				    break;
			    }
                            
                        } /* end if previous extent is a hole */
                    } /* end else if at beginning of extent */
                    
                    /*
                     * If at the end of an extent, then create the new
                     * extent after this one.
                     */
                    else if (extSize - readLength <= 0)
                    {
                        extentp->extentSize -= readLength; /* decrement size */
                        /*
                         * If the next extent is a hole, then just add this
                         * page to it.
                         */
                        if (NULL != extentp->next && PERM_HOLE_START == extentp->next->diskBlock)
                        {
                            extentp->next->extentSize += readLength;
                            extentp->next->byteInFile -= readLength;
                        }
                        else
                        {
                            /*
                             * Create a new extent after this one.
                             */
                            newExtp = NULL;
                            status = create_extent_element (&newExtp, -2, (LBNT)-2,
                                 readLength, 
                                 (extentp->byteInFile + extentp->extentSize));
                            if (NO_MEMORY == status)
                            {
                                returnstatus = NO_SPACE;
				goto _return;
                            }
                            /*
                             * Insert the new extent after this one
                             * and point to it. This will cause us to
                             * read the extent with the hole next and
                             * lseek past the hole.
                             */
                            newExtp->next = extentp->next;
                            extentp->next = newExtp;
                            prevExtp = extentp;
                            extentp = newExtp;
                        } /* end if next extent is a hole */
                    } /* end if at end of extent */
                    
                    /*
                     * Otherwise, split this extent and create the new one
                     * in the middle.
                     */
                    else
                    {
                        /*
                         * Create an extent for the piece in front of
                         * the page with the I/O error.
                         */
                        newExtp = NULL;
                        status = create_extent_element (&newExtp, extentp->volume,
                                             extentp->diskBlock, 
                                             (extentp->extentSize - extSize),
                                             extentp->byteInFile);
                        if (NO_MEMORY == status)
                        {
                            returnstatus = NO_SPACE;
			    goto _return;
                        }
                        /*
                         * If the previous extent exist then make it point
                         * to the new extent, otherwise point the list head
                         * at the new extent.
                         */
                        if (NULL != prevExtp)
                        {
                            prevExtp->next = newExtp;
                        }
                        else
                        {
                            tagEntryp->extentHead = newExtp;
                        }
                        newExtp->next = extentp;
                        prevExtp = newExtp;
                        /*
                         * Changes values in the current extent to subtract
                         * off the extent we just created and the extent to
                         * be created to cover the page with the I/O error.
                         */
                        extentp->byteInFile += newExtp->extentSize;
                        extentp->extentSize -= newExtp->extentSize + readLength;
                        extentp->diskBlock += newExtp->extentSize/512 
                                              + readLength/512;

                        /*
                         * Now create an extent for the page with the I/O error.
                         */
                        newExtp = NULL;
                        status = create_extent_element (&newExtp, -2, (LBNT)-2,
                                     readLength, extentp->byteInFile);
                        if (NO_MEMORY == status)
                        {
                            returnstatus = NO_SPACE;
			    goto _return;
                        }
                        /*
                         * Insert the new extent between the previous
                         * extent and this one. 
                         */
                        newExtp->next = extentp;
                        prevExtp->next = newExtp;
                        prevExtp = newExtp;
                        extentp->byteInFile += readLength;
                        
                        /* Lseek past the hole in the file. */
			switch (Options.outputFormat)
			{
			    case F_DISK:
				lseek (fd, (off_t) extentp->byteInFile,
				       SEEK_SET);
				break;
			    case F_TAR:
				status = output_tar_data(tarFileSize,
						extentp->byteInFile -
						  tmpByteInFile,
						TRUE, NULL, &bytesWritten);
				if (status != SUCCESS)
				{
				    S_SET_PARTIAL (tagEntryp->status);
				    returnstatus = PARTIAL;
				    goto _return;
				}
				break;
			    default:
				writemsg(SV_DEBUG, catgets(_m_catd,
					 S_SALVAGE_1, SALVAGE_262,
					 "Invalid output format '%d'.\n"),
					 Options.outputFormat);
				break;
			}
                        
                    } /* end if in middle of extent */
                } /* end else restoring partial files */
            } /* end if rwstatus is FAILURE */
            
            bytePosition += readLength;
            extLBN += readLength/512;
            extSize -= readLength;
            
        } /* end while extSize > 0 */
        
        prevExtp = extentp;
        extentp = extentp->next;
        
    } /* end while extentp != NULL */
    
 _return:
    if (F_TAR == Options.outputFormat)
    {
	if (bytesWritten < tarFileSize)
	{
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_264,
		     "predicted %ld bytes in file, only wrote %ld bytes.  Zero-filling %d bytes at end.\n"),
		     tarFileSize, bytesWritten, tarFileSize - bytesWritten);

	    status = output_tar_data(tarFileSize, tarFileSize -
				     bytesWritten, TRUE, NULL,
				     &bytesWritten);
	    if (status != SUCCESS)
	    {
		/* This should be an assertion.  Can't get this. */
		returnstatus = FAILURE;
	    }
	}

	if (pad = (bytesWritten % BLOCKSIZE))
	{
	    pad = (BLOCKSIZE - pad);
	}

	buf_pad((OFFSET) pad);
    }
    return returnstatus;
}
/* end restore_file_data */


/*
 * Function name: restore_attrs (3.7.6)
 *
 * Description:
 *  This routine restores the uid, gid, protection mode, atime, ctime,
 *  and mtime for a file. This routine is not called for symlinks.
 *
 * Input parameters: 
 *  pathName: pointer for pathname of file being restored.
 *  localPath: pointer for local path name of file being restored.
 *  attrp: pointer to attributes for this file.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */
int restore_attrs (char          *pathName,
                   char          *localPath,
		   tagAttributeT *attrp)
{
    char *funcName = "restore_attrs";
    struct timeval fileTimes[2];  /* time values for utimes call */
    int status = SUCCESS;          /* return status */

    if (-1 == chown (localPath, attrp->uid, attrp->gid))
    { 
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_209, 
                 "%s: can't restore uid/gid\n"), pathName);
        status = FAILURE;
    }
    if (-1 == chmod (localPath, attrp->mode & 07777))
    {
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_210, 
                 "%s: can't restore file permissions\n"), pathName);
        status = FAILURE;
    }
    
    /*
     * Copy atime and mtime from attributes into fileTimes for utimes call.
     */
    fileTimes[0] = attrp->atime;
    fileTimes[1] = attrp->mtime;
    
    if (-1 == utimes (localPath, fileTimes))
    {
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_211, 
                 "%s: can't restore file atime/mtime\n"), pathName);
        status = FAILURE;
    }
    return status;
}
/* end restore_attrs */


/*
 * Function name: restore_proplist (3.7.7)
 *
 * Description:
 *  This routine restores any property lists for a file.
 *
 * Input parameters: 
 *  tagEntryp: pointer to tag pointer array entry for this file.
 *  pathName: pointer to the pathname for this file.
 *  localPath: pointer to the local path name for this file.
 *
 * Returns:
 *  Status value - SUCCESS, NO_SPACE, or FAILURE. 
 */
int restore_proplist (filesetTagT *tagEntryp,
		      Stat	  *pStatbuf,
		      char        *pathName,
		      char        *localPath)
{
    char *funcName = "restore_proplist";
    char *buffer;    /* pointer to buffer for setproplist call */
    char *ptr;       /* pointer to buffer for proplist calls */
    int plistSize;   /* total size of property list */
    int retSize;     /* size after adding proplist entries */
    int retry;       /* retry flag for out of space failure */
    int status;      /* returned status from setproplist */
    int align;	     /* padding alignment returned from writetar */
    tagPropListT *currPlistp; /* pointer to current proplist in list */

    /*
     * Loop through property list linked list
     */
    plistSize = 0;
    currPlistp = tagEntryp->props;
    
    if (NULL == currPlistp)
    {
	/*
	 * This could be a DELETED proplist.
	 */
	return SUCCESS;
    }

    while (NULL != currPlistp)
    {
        /*
         * Compute size of entry using name length, value length, and 
         * size of each field and add to plistSize.
         */
        plistSize += sizeof_proplist_entry (
                                     currPlistp->pl_union.pl_complete.nameBuffer,
                                     currPlistp->pl_union.pl_complete.valueLen);
        currPlistp = currPlistp->next;
    }
    
    /*
     * This routine is called only if we have a property list, so
     * plistSize should not be zero when we get here.
     * Allocate the space for a buffer of plistSize bytes.
     */
    buffer = (char *)salvage_malloc (plistSize);
    
    if (NULL == buffer)
    {
        writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_212, 
                  "Can't allocate space for proplist for file '%s'\n"),
                  pathName);
        return NO_SPACE;
    }
    
    ptr = buffer;
    retSize = 0;
    /*
     * For each entry in property list linked list, copy it into buffer.
     */
    currPlistp = tagEntryp->props;
    
    while (NULL != currPlistp)
    {
        /*
         * Add the proplist entry to the list.
         */

        retSize += add_proplist_entry (currPlistp->pl_union.pl_complete.nameBuffer, 
                                       0,
                                       currPlistp->pl_union.pl_complete.valueLen, 
                                       currPlistp->pl_union.pl_complete.valBuffer, 
                                       &ptr);
        currPlistp = currPlistp->next;
    }

    /*
     * If the sizes don't match, then some problem occurred.
     */    
    if (retSize != plistSize)
    {
        free (buffer);
        return FAILURE;
    }
    
    if (F_TAR == Options.outputFormat) {
	num_extended_attribute_bytes = plistSize;
	align = writetar(localPath, pStatbuf);
	if (align == -1)
	{
	    return FAILURE;
	}
    }
    do
    {
        retry = 0;
        
        /*
         * Restore the property list
         */
	switch (Options.outputFormat)
	{
	    case F_DISK:
		status = setproplist (localPath, 0, plistSize, buffer);
		break;
	    case F_TAR:
		status = plistSize;
		outpropdata(buffer, plistSize);
		break;
	    default:
		writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_262,
			 "Invalid output format '%d'.\n"),
			 Options.outputFormat);
		break;
	}
        
        if (-1 == status)
        {
            /*
             * If the error we got was lack of disk space, then see
             * if the user wants to continue after they create some
             * space for us.
             */
            if (ENOSPC == errno)
            {
                writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_192, 
                          "No room to create file '%s'\n"), pathName);
                writemsg (SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_213, 
                          "Add more space to fileset and continue or abort.\n"));
                if (want_abort ())
                {
                    free (buffer);
                    return NO_SPACE;
                }
                else
                {
                    retry++;
                }
            }
            else
            {
                free (buffer);
                return FAILURE;
            }
        }
        else if (status != plistSize)
        {
            free (buffer);
            return FAILURE;
        }
    }
    while (0 != retry);

    free (buffer);    
    return SUCCESS;
}
/* end restore_proplist */


/*
 * Function name: restore_symlink (3.7.8)
 *
 * Description:
 *   This routine restores a symbolic link.
 *
 * Input parameters:
 *  tagEntryp: pointer to entry in fileset tag pointer array for link.
 *  pathName: pointer to pathname of link.
 *  localPath: pointer to local path name of link.
 * 
 * Returns:
 *  Status value - SUCCESS, NO_SPACE, or FAILURE. 
 */
int restore_symlink (filesetTagT *tagEntryp,
		     char        *pathName,
		     char        *localPath)
{
    char *funcName = "restore_symlink";
    mode_t tmpMask;  /* temporary umask */
    int retry;       /* retry flag for out of space failure */
    int status;      /* returned status from called routines */
    int tagNum;      /* tag number for log messages */
    struct timeval file_times[2];  /* time values for utimes call */
    
    tmpMask = 0;
    tagNum = tagEntryp->firstInstance->tagNum;
    
    /*
     * If we don't have the link in additional attributes, then return.
     */
    if (NULL == tagEntryp->addAttrs)
    {
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_214, 
                 "%s: %d : symlink has no name to link to\n"), pathName, tagNum);
        return FAILURE;
    }
    
    /*
     * For the symbolic link we can only set the protection mode at
     * creation by using umask.
     */
    do
    {
        retry = 0;
        /*
         * If we have a new mode, then set the umask to it while saving the
         * current umask in tmpMask. Otherwise just let the current umask
         * take affect when creating the symlink.
         */
        if (NULL != tagEntryp->attrs)
        {
            tmpMask = umask (~tagEntryp->attrs->mode);
        }
        status = symlink ((char *)tagEntryp->addAttrs, localPath);
        
        /*
         * Restore umask if we changed it
         */
        if (0 != tmpMask)
        {
            umask (tmpMask);
        }
        
        /*
         * If there was an error from symlink, see if we ran out of disk space.
         */
        if (-1 == status)
        {
            /*
             * If the error we got was lack of disk space, then see
             * if the user wants to continue after they create some
             * space for us.
             */
            if (ENOSPC == errno)
            {
                writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_192, 
                          "No room to create file '%s'\n"), pathName);
                writemsg (SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_193, 
                          "Add more space to fileset and continue or abort\n"));
                if (want_abort ())
                {
                    return NO_SPACE;
                }
                else
                {
                    retry++;
                }
            }
            else
            {
                    return FAILURE;
            }
        }
    }
    while (retry == 1);

    /*
     * If we have attributes, then set the uid and gid and restore times.
     */
    if (NULL != tagEntryp->attrs)
    {
        if (-1 == lchown (localPath, tagEntryp->attrs->uid, 
                          tagEntryp->attrs->gid))
        {
            fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_215, 
                     "%s: %d : can't set uid/gid for symlink\n"), pathName, tagNum);
            writemsg (SV_NORMAL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_216, 
                      "Error restoring attributes for file '%s'\n"), pathName);
        }
    }
    else
    {
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_217, 
                 "%s: %d : no attributes for symlink\n"), pathName, tagNum);
        writemsg (SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_205, 
                  "Attributes not found for file '%s'\n"), pathName);
    }
    return SUCCESS;
}
/* end restore_symlink */


/*
 * Function name: restore_special (3.7.9)
 *
 * Description:
 *  This routine restores a fifo or device special file.
 *
 * Input parameters:
 *  nodep: pointer to node in fileset directory tree.
 *  pathName: pointer to name for file.
 *  localPath: pointer to local path name for file.
 *  tagEntryp: pointer to fileset tag array entry for file.
 *
 * Returns:
 *  Status value - SUCCESS, NO_SPACE, or FAILURE. 
 */

int restore_special (filesetTreeNodeT *nodep,
                     char             *pathName,
                     char             *localPath,
                     filesetTagT      *tagEntryp)
{
    char *funcName = "restore_special";
    int retry;  /* retry flag for out of space failure */
    int status; /* return status from system calls */
    mode_t mode;  /* needed in case we don't have attrs */

    /*
     * If we have attributes, then get the mode needed to create the
     * special file from there. Otherwise use the default.
     */
    if (NULL != tagEntryp->attrs)
    {
        mode = tagEntryp->attrs->mode;
    }
    else
    {
	/* Set default mode */
	if (FT_FIFO == nodep->fileType)
	{
	    mode = S_IFIFO | S_IRWXU;
	}
	else
	{
	    /* No way to tell character vs. block, so pick one. */
	    mode = S_IFCHR | S_IRWXU;
	}
    }

    do
    {
        retry = 0;
        errno = 0;
        
        if (FT_FIFO == nodep->fileType)
        {
            status = mkfifo (localPath, mode);
        }
        else
        {
            /*
             * We need rdev stored in addAttrs to create a device file.
             * If it's not there, print an error message and return.
             */
            if (NULL == tagEntryp->addAttrs)
            {
                return FAILURE;
            }
            status = mknod (localPath, mode, *(dev_t *)(tagEntryp->addAttrs));
        }
        if (0 != status)
        {
            /*
             * If the error we got was lack of disk space, then see
             * if the user wants to continue after they create some
             * space for us.
             */
            if (ENOSPC == errno)
            {
                writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_192, 
                          "No room to create file '%s'\n"), pathName);
                writemsg (SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_193, 
                          "Add more space to fileset and continue or abort\n"));
                if (want_abort ())
                {
                    return NO_SPACE;
                }
                else
                {
                    retry++;
                }
            }
            else
            {
                return FAILURE;
            }
        }
    }
    while (0 != retry);

    if (NULL != tagEntryp->attrs)
    {
        if (SUCCESS != restore_attrs (pathName, localPath, tagEntryp->attrs))
        {
            fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_194, 
                     "%s: %d : Error setting file attributes\n"), 
                     pathName, nodep->tagNum);
            writemsg (SV_NORMAL, catgets(_m_catd, S_SALVAGE_1, SALVAGE_216, 
                      "Error restoring attributes for file '%s'\n"), pathName);
        } 
    }
    else
    {
        fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, SALVAGE_218, 
                 "%s: %d : No attributes available for file\n"), 
                 pathName, nodep->tagNum);
        writemsg (SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_205, 
                  "Attributes not found for file '%s'\n"), pathName);
    }

    return SUCCESS;
}
/* end restore_special */

/*
 * Function name: restore_hardlink (3.7.10)
 *
 * Description:
 *  This routine restores a hard link.
 *
 * Input parameters:
 *  nodep: pointer to node in the fileset directory tree.
 *  pathName: name for this file.
 *  localPath: local path name for this file.
 *  tagEntryp: pointer to fileset tag pointer array entry for node.
 *
 * Returns:
 *  Status value - SUCCESS, NO_SPACE, or FAILURE. 
 */
 
int restore_hardlink (filesetTreeNodeT *nodep,
                      char             *pathName,
                      char             *localPath,
                      filesetTagT      *tagEntryp)
{
    char *funcName = "restore_hardlink";
    char *fileName;  /* pointer to path name for existing first instance node */
    int retry;       /* retry flag for out of space failure */
    int status;      /* returned status */

    /*
     * Get the pathname of the currently existing file. This should always
     * exist since we won't get here unless the file status is RECOVERED.
     */
    status = get_pathname (&fileName, tagEntryp->firstInstance);
    if (FAILURE == status)
    {
        return FAILURE;
    }
    
    do
    {
        retry = 0;
        errno = 0;
        
        /*
         * Link the current pathname to the existing file.
         */
        status = link (fileName, localPath);
        if (status < 0)
        {
            /*
             * If the error we got was lack of disk space, then see
             * if the user wants to continue after they create some
             * space for us.
             */
            if (ENOSPC == errno)
            {
                writemsg (SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_192, 
                          "No room to create file '%s'\n"), pathName);
                writemsg (SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_193, 
                          "Add more space to fileset and continue or abort\n"));
                if (want_abort ())
                {
                    status = NO_SPACE;
                }
                else
                {
                    retry++;
                }
            }
            else
            {
                status = FAILURE;
            } /* end if errno is ENOSPC */
        }
        else
        {
            status = SUCCESS;
        } /* end if status < 0 */
    }
    while (0 != retry);
    free (fileName);
    
    return status;
}
/* end restore_hard_link */


/*
 * Function name: reset_dir_dates (3.7.11)
 *
 * Description:
 *  This routine resets the access/modify dates for directories. We need
 *  to do this because creating files in a directory changes these dates
 *  and the user should really see the same dates as had been on the disk.
 *
 * Input parameters:
 *  nodep: pointer to tree node for this file.
 *  recoverData: pointer to data passed from recover_fileset.
 *
 * Returns:
 *  Status value - SUCCESS. 
 */
 
int reset_dir_dates (filesetTreeNodeT *nodep,
                     recoverDataT     *recoverData)
{
    char *funcName = "reset_dir_dates";
    filesetTagT *tagEntryp;    /* pointer to tag array entry for node */
    char *pathName;            /* pathname for directory */
    struct timeval fileTimes[2];  /* time values for utimes call */    
    int status;                /* status returned from calls */
    char *localPath;           /* pointer to local path name */
    char newDirPath[PATH_MAX+2];      /* new directory path */
    int stIdx = 0;     /* start index for operations on path name */
    int endIdx = PATH_MAX-1;  /* end index for operations on path name */
    int dirIdx = 0;        /* index for path name copy */
    
    /* Get the fileset tag pointer array entry for nodep in tagEntryp. */
    
    tagEntryp = recoverData->tagArray[nodep->tagNum];
    
    /*
     * If for some reason there is no tag pointer or it's marked as ignore
     * or dead, then just return. This probably shouldn't happen at this
     * point, but we better check for it anyways.
     */
    
    if (NULL == tagEntryp || IGNORE == tagEntryp || DEAD == tagEntryp)
    {
        return SUCCESS;
    }
    
    /* If it's a directory, then process it, otherwise just return. */
    
    if (FT_DIRECTORY == tagEntryp->fileType)
    {
    
        /* Only reset the times if we have attributes */
        if (NULL != tagEntryp->attrs)
        {
        
            /* Get the path of the directory for utimes. */
        
            status = get_pathname (&pathName, nodep);
            if (SUCCESS == status)
            {
                localPath = pathName;    /* Set local path to file name. */

                /* 
                 * If name is too long to do a create, then chop it up here and 
                 * cd to directory further down the tree, until name is less than 
                 * PATH_MAX. Need to cd back to recover directory at the end of 
                 * this routine.
                 */
                while (strlen (localPath) >= PATH_MAX)
                { 

                    /*
                     * The pathname is too long, so break it up into
                     * sections and change directory to each section.
                     * All preceding directories should have been
                     * previously created.
                     */
                     
                    bzero (newDirPath, sizeof (newDirPath));
            
                    /* 
                     * Back up from the end index until we hit a '/'. 
                     * We should hit one because it's pretty hard to create
                     * a path longer than PATH_MAX without getting a "name
                     * too long" error along the way. However, to be paranoid,
                     * we'll make sure we don't go past the current stIdx.
                     */
                    while ((pathName[endIdx] != '/') && endIdx > stIdx)
                    {
                        endIdx--;
                    }
  
                    if (pathName[endIdx] == '/')
                    {
            
                        /* Copy the new directory path and chdir to it. */
                        strncpy (newDirPath, pathName+stIdx, endIdx-stIdx);            
                        if (-1 == chdir (newDirPath))
                        {
                            writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_24, 
                                     "Can't change directory to '%s'\n"), newDirPath);
                            return FAILURE;
                        }
            
                        /*
                         * Start the local path one character past the '/' we found
                         * above and set stIdx there, too. Set the end at PATH_MAX-1 
                         * from the start and check the size of the local path again.
                         */
                        localPath = pathName + endIdx + 1;
                        stIdx = endIdx + 1;
                        endIdx += PATH_MAX - 1; 
                    }
                    else
                    {
                        /*
                         * If we got out of the loop without finding a '/', then
                         * just set localPath to the original name and let the 
                         * utimes call error out.
                         */
                        localPath = pathName;
                        chdir (Options.recoverDir);
                        stIdx = 0;    /* flag that we don't need to chdir later */
                        break;
                    } /* end if we found a '/' */
                } /* end while path is too long */
   
                /*
                 * Copy atime and mtime from attributes into fileTimes 
                 * for utimes call.
                 */
                fileTimes[0] = tagEntryp->attrs->atime;
                fileTimes[1] = tagEntryp->attrs->mtime;
    
                if (-1 == utimes (localPath, fileTimes))
                {
                    fprintf (Options.logFD, catgets(_m_catd, S_SALVAGE_1, 
                             SALVAGE_219, 
                             "%s: %d :can't reset directory atime/mtime\n"),
                             pathName, nodep->tagNum);
                }
            } /* end if we got a path name */
	    free (pathName);
	    
            /* If we did a chdir down into the tree, then reset to the top. */
            if (stIdx != 0)
            {
                chdir (Options.recoverDir);
            }
        } /* end if we have attributes */
    } /* end if we found a directory */
    return SUCCESS;
}

/* end reset_dir_dates */

/*
 * Function name: output_tar_data (3.7.x)
 *
 * Description:
 *  This routine does the necessary calculations to write a chunk of
 *  file data in tar format.  It has been separated out from
 *  restore_file_data() due to the number of times this code would
 *  have to be repeated in that routine.
 *
 * Input parameters:
 *  fileSize: size tar header thinks this file should be.
 *  bufSize: number of bytes to be written in this call.
 *  zeroFill: boolean whether to zero-fill the data or copy data from buffer.
 *  buffer: pointer to buffer where data is to be copied from.
 *  bytesWritten: pointer to number of bytes written to the archive so far.
 *
 * Side Effects:
 *  bytesWritten is incremented by the number of bytes added to the archive.
 *  Either buf_out_avail or buf_use can cause the program to exit.
 *
 * Returns:
 *  Status value - SUCCESS or PARTIAL. 
 */
 
int output_tar_data (long fileSize, long bufSize, int zeroFill,
		     char *buffer, long *bytesWritten)
{
    char *funcName = "output_tar_data";
    unsigned int avail;	 /* Amount of free space remaining in tar buffer */
    char *tarbuf;	 /* Pointer into the tar buffer for writing data */
    unsigned int chunk;	 /* Amount of data to write to archive this pass */
    int i;

    /*
     * bufSize is reduced by the amount written each pass.  Keep
     * looping until everything in the buffer has been written.
     */
    while (bufSize > 0)
    {
	/*
	 * Set tarbuf to the next free space in tar's global buffer,
	 * and find out how much space is available in the tar global
	 * buffer.  If the tar global buffer was full, it's flushed to
	 * the archive and cleared.
	 */
	avail = buf_out_avail(&tarbuf);
	chunk = MIN(bufSize, avail);
	bufSize -= chunk;

	if (zeroFill)
	{
	    for (i = 0 ; i < chunk ; i++)
	    {
		tarbuf[i] = '\0';
	    }
	}
	else
	{
	    if (buffer == NULL)
	    {
		writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_263,
		     "NULL buffer passed into output_tar_data.\n"));
	    }
	    else
	    {
		/*
		 * Copy local buffer data to tar's global buffer, then
		 * reset local buffer pointer to next set of data to be
		 * copied after this chunk has been written.
		 */
		memcpy(tarbuf, buffer, (size_t)chunk);
		buffer += chunk;
	    }
	}

	/*
	 * We cannot write more bytes than the tar header thinks there
	 * are in this file.  If asked to, truncate this buffer to the
	 * predicted fileSize, then return PARTIAL so error message can
	 * be printed.
	 */
	if ((*bytesWritten + chunk) > fileSize)
	{
	    chunk = fileSize - *bytesWritten;
	    if (chunk > 0)
	    {
		/*
		 * buf_use tells tar that 'chunk' bytes have been filled in
		 * the tar global buffer, and tar then resets its pointers
		 * to remember this.
		 */
		buf_use(chunk);
		/*
		 * Ensure the total number of bytes written to the archive is
		 * equal to the number of bytes predicted for this file.
		 */
		*bytesWritten += chunk;
	    }
	    return PARTIAL;
	}

	/*
	 * buf_use tells tar that 'chunk' bytes have been filled in
	 * the tar global buffer, and tar then resets its pointers
	 * to remember this.
	 */
	buf_use(chunk);
	/*
	 * Ensure the total number of bytes written to the archive is
	 * equal to the number of bytes predicted for this file.
	 */
	*bytesWritten += chunk;
    }
    return SUCCESS;
}
/* end output_tar_data */

/* end salvage_recover.c */
