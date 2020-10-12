# :coding: utf-8
#
# ftrack action for sending a client review to participants using accsyn
#
# Requirements:
#
# - Python 3 with ftrack and accsyn API.s (pip install ftrack-python-api 
#   accsyn-pyton-api). (Compatible with Python 2 with minor changes)
# - FTRACK_* and ACCSYN_* environment variables to be set properly prior to 
#   invocation.
# - Accsyn server, configured to server a root share.
# - Two ftrack components published at each version, one \*.mov as preview and 
#   a one file sequence.
#
# Use/modify/distribute freely, at your own risk, no warranties or liabilities 
# are provided. 
#
# For more sample code, visit our GitHub: github.com/accsyn. 
# Website: https://accsyn.com.
#
# Author: Henrik Norin, accsyn/HDR AB, (c)2020
# 

import os
import json
import logging
import threading
import traceback
import time
import datetime

import ftrack_api

import accsyn_api

#logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.DEBUG) # PUBREMOVE

identifier = 'accsynreviewdistribute_v1.action'


# Replace this with the ID of accsyn desktop app/server running on same 
# machine as action, or on another machine with access to files at their 
# paths.

ACCSYN_CLIENT_ID='5d84a31ace1bf9913a17cc20'

class AccsynReviewDistributeAction():


    def __init__(self):
        self.session = ftrack_api.Session(auto_connect_event_hub=True)
        #self.session = ftrack_api.Session(auto_connect_event_hub=True)
        self.identifier = "AccsynReviewDistributeAction_v1"
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
        )
        self.excluded_locations = [
            'ftrack.origin', 
            'ftrack.connect', 
            'ftrack.unmanaged', 
            'ftrack.server', 
            'ftrack.review', 
        ]


    def log_and_return(self, s, retval=False):
        logging.info(s)
        return {
                'success': retval,
                'message': s
        }

    def register(self):
        self.session.event_hub.subscribe(
                'topic=ftrack.action.discover',
                self.discover
        )

        self.session.event_hub.subscribe (
                'topic=ftrack.action.launch and data.actionIdentifier={0}'
                .format(
                        self.identifier
                ),
                self.launch
        )

    def discover(self, event):
        data = event['data']

        # Can only be run on review sessions
        selection = data.get('selection', [])
        self.logger.debug(
            '(ARD) Discover; Got selection: {0}'.format(selection))

        if len(selection) != 1:
            return self.log_and_return('(ARD) Cannot run Action - nothing '
                'selected!', True)

        selected = selection[0]
        if selected['entityType'].lower() != "reviewsession":
            return self.log_and_return('(ARD) Cannot only run Action on a '
                'Review Session!', True)

        return {
            'items': [{
                    'label': 'accsyn review session distribute',
                    'actionIdentifier': self.identifier
            }]
        }

    def launch(self, event):
        #self.session._local_cache.clear()
        self.logger.debug('(AS) Launch; Event items: {}'.format(str(event.items())))

        selection = event['data'].get('selection', [])
        self.logger.info('(AS) Launch; Got selection: {0}'.format(selection))

        # Extract participants
        selected = selection[0]
        ft_review_session = self.session.query('ReviewSession where id={}'
            .format(selected['entityId'])).one()
        if ft_review_session is None:
            return self.log_and_return(
                'Review session could not be loaded!',False)
        if len(ft_review_session['review_session_objects']) == 0:
            return self.log_and_return(
                'Review session is empty - contains no versions!',False)
        participants = []
        for ft_review_session_invitee in ft_review_session['review_session_invitees']:
            participants.append(ft_review_session_invitee)
        if len(participants)==0:
            return self.log_and_return(
                'Review session has no invitees/collaborators!', False)
        share_name = "{}-review".format(ft_review_session['project']['name'])
        share_path = "{}{}review".format(ft_review_session['project']['name'], os.sep)
        directory = os.path.join(datetime.datetime.now().strftime("%Y%m%d"),ft_review_session['name'].lower().replace(' ','_'))

        if 'values' in event['data']:
            values = event['data']['values']

            recipients = values['recipients'].split(",")
            share_name = values['share_name']
            share_path = values['share_path']
            directory = values['directory']
            additional_files = values['additional_files']

            if len(recipients)==0:
                return self.log_and_return(
                    'No recipients!',False)
            for recipient in recipients:
                s = (recipient or "").strip()
                if len(s) == 0 or s.find("@") == -1:
                    return self.log_and_return(
                        'Empty or invalid recipient: {}!'.format(
                            recipient),False)
            if len(share_name)==0:
                return self.log_and_return(
                    'No intermediate accsyn share name given!',False)

            # Run in separate thread so we do not lock up action subsystem
            thread = threading.Thread(target=self.run, args=(
                event, 
                ft_review_session, 
                recipients, 
                share_name, 
                share_path, 
                directory, 
                additional_files))
            thread.start()

            #self.run(event, selection)
            return self.log_and_return(
                'Distribution of version(s) initiated, check ftrack '
                'job and accsyn app for progress!',True)
        else:

            widgets = [
                {
                    'value':'This Action distributes files from components @ '
                    'review session versions to participants:<br><br>'
                        '<ul>'
                            '<li>Harvests all components from review session '
                            'versions.</li>'
                            '<li>Extract previews (.mov|.mp4).</li>'
                            '<li>Extract file sequences (.exr|.dpx|.tif(f)).'
                            '</li>'
                            '<li>Makes sure a review share exists @ accsyn.'
                            '</li>'
                            '<li>Creates an accsyn transmit job to review '
                            'session participants.</li>'
                        '</ul><br>'
                        '<br>'
                        'Please confirm recipients and intermediate share:',
                    'type':'label'
                },
                {
                    'label': 'Recipients(comma separated list of email '
                    'addresses):',
                    'name': 'recipients',
                    'value': ",".join([p['email'] for p in participants]),
                    'type': 'text'
                },
                {
                    'label': 'Intermediate accsyn share name:',
                    'name': 'share_name',
                    'value': share_name,
                    'type': 'text'
                },
                {
                    'label': 'Intermediate accsyn share path:',
                    'name': 'share_path',
                    'value': share_path,
                    'type': 'text'
                },{
                    'label': 'Intermediate accsyn directory:',
                    'name': 'directory',
                    'value': directory,
                    'type': 'text'
                }
            ]

            widgets.extend([
                {
                    'label': 'Additional files (one entry per row):',
                    'name': 'additional_files',
                    'value': '',
                    'type': 'textarea'
                }
            ])

            return {'items': widgets }

    def normpath(self, p_raw):
        return p_raw.replace("/", os.sep).replace("\\", os.sep)

    def version_ident(self, ft_version):
        ft_task = ft_version['task']
        context = ft_version['asset']['parent']
        return '{}'.format(
            '_'.join([link['name'] for link in context['link']]),
            ft_task['name'],
            ft_version['version']
        )

    def run(self, event, ft_review_session, recipients, share_name, share_path, directory, additional_files):

        session = ftrack_api.Session(auto_connect_event_hub=False)
        accsyn_session = accsyn_api.Session()

        logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__ + '.thread'
        )
        job = None
        user = event['source']['user']

        def info(s):
            logger.info(s)
            if job:
                job['data'] = json.dumps({'description': s})
                session.commit()
            return s

        def warning(s):
            logger.warning(s)
            if job:
                job['data'] = json.dumps({'description': s})
                session.commit()
            return s

        def web_message(s):
            session.event_hub.publish(
                ftrack_api.event.base.Event(
                    topic='ftrack.action.trigger-user-interface',
                    data=dict(
                        type='message',
                        success=False,
                        message=(s)
                    ),
                    target='applicationId=ftrack.client.web and user.id={0}'
                    .format(user['id'])
                ),
                on_error='ignore'
            )

        def error(s):
            warning(s)
            web_message('[ERROR] {}'.format(s))
            return s
            
        info('Creating ftrack job..')

        # Create a new running Job.     
        job = session.create(
            'Job',
            {
                'user': session.get('User', user['id']),
                'status': 'running',
                'data': json.dumps({
                    'description': 'Initialising Accsyn send...'
                    }
                )
            }
        )
        session.commit()

        job_final_status = 'done'
        component_count = 0
        try:

            info("Harvesting components..")

            previews = []
            sequences = []

            selection = event['data'].get('selection', [])

            for ft_review_session_object in \
                ft_review_session['review_session_objects']:
                ft_version = ft_review_session_object['asset_version']

                version_had_files = False
                # Collect all the components attached to the selected entity
                for ft_component in session.query(
                    'Component where version.id={}'.format(
                        ft_version['id']
                    )
                ):
                    p_raw = None
                    for d in ft_component['component_locations']:
                        ft_location = d['location']
                        if ft_location['name'] in self.excluded_locations:
                            continue
                        try:
                            p_raw = ft_location.get_filesystem_path(
                                ft_component)
                        except:
                            logging.warning('   Could not extract path for '
                                '{}_{} @ location: {}; {}'.format(
                                self.version_ident(ft_version),
                                ft_component['name'], 
                                ft_location['name'], 
                                traceback.format_exc()))
                            continue
                        if 0<len(p_raw or ''):
                            break
                    if p_raw:
                        p = self.normpath(p_raw)
                        # Preview or file sequence
                        filename = os.path.basename(p)
                        prefix, ext = os.path.splitext(filename)
                        if 0<len(ext or ''):
                            # Add custom previwable file formats here
                            if ext.lower() in ['.mov','.mp4']:
                                previews.append(p_raw)
                                version_had_files = True
                            # Add custom file sequence file formats here
                            #elif ext.lower() in ['.exr','.dpx','.tif','.tiff']:
                            elif ext.lower() in ['.exr','.dpx','.tif','.tiff','.jpg']: # PUBREMOVE
                                sequences.append(p_raw)
                                version_had_files = True
                    else:
                        self.logger.debug('{}_{}; No files found.'.format(
                            self.version_ident(ft_version), 
                            ft_component['name']))
                if not version_had_files:
                    self.logger.warning('{}; No files found.'.format(
                        self.version_ident(ft_version)))

            if 0<len(additional_files.strip()):
                for p_raw in additional_files.split('\n'):
                    if 0<len(p_raw or ''):
                        # Add custom previwable file formats here
                        if ext.lower() in ['.mov','.mp4']:
                            previews.append(p_raw)
                        # Add custom file sequence file formats here
                        elif ext.lower() in ['.exr','.dpx','.tif','.tiff']:
                            sequences.append(p_raw)

            if len(previews) == 0 and len(sequences) == 0:
                error('[ERROR] No preview/sequences/additional files found! '
                    'Make sure version(s) have published components.')
                job_final_status = 'failed'
                return

            # Remove this if server is on prem and serving file system file(s)
            # resides on.
            info('Checking share {}...'.format(share_name))
            share = accsyn_session.find_one('Share where code="{}"'.format(
                share_name))
            if share is None:
                warning('Share {} does not exist, creating...'.format(
                    share_name))
                share = accsyn_session.create('Share',{
                    'code':share_name,
                    'path':share_path
                })
                info('Created share {} with id {}.'.format(
                    share['code'],share['id']))

            info('Building accsyn job out of {} files(s)...'
                .format(len(previews)+len(sequences)))

            accsyn_job_data = {
                'name':'{}'.format(
                    ft_review_session['name']),
                'tasks':[],
                'recipients':recipients
            }
            
            file_count = 0

            # Empty this if server is on prem and serving file system file(s)
            # resides on.
            source_party = 'client={}:'.format(ACCSYN_CLIENT_ID)

            # First, add previews at high priority - send these first.
            for p_raw in previews:
                logging.info('   Adding preview file: {}'.format(p_raw))

                # Put sequence parent directory in share and intermediate 
                # directory at server, at user put in intermediate dir.
                intermediate_path = 'share={}{}{}{}{}'.format(
                    share_name,
                    os.sep,
                    directory,
                    os.sep,
                    os.path.basename(self.normpath(p_raw))
                )
                destination_path = '{}{}{}'.format(
                    directory,
                    os.sep,
                    os.path.basename(self.normpath(p_raw))
                )

                accsyn_job_data['tasks'].append({
                    'source':'{}{}'.format(source_party, p_raw),
                    'destination':'{}:{}'.format(
                        intermediate_path, destination_path),
                    'priority':999
                })
                # If server is on prem and serving file system file(s) resides 
                # on, replace destination with:
                # 
                # 'destination':'{}'.format(destination_path)

                file_count += 1

            # Lastly, add sequences
            for p_raw in sequences:
                logging.info('   Adding sequence files: {}'.format(p_raw))

                # Put sequence parent directory in share and intermediate 
                # directory at server, at user put in intermediate dir.
                p = os.path.dirname(self.normpath(p_raw))
                intermediate_path = 'share={}{}{}{}{}'.format(
                    share_name,
                    os.sep,
                    directory,
                    os.sep,
                    os.path.basename(p)
                )
                destination_path = '{}{}{}'.format(
                    directory,
                    os.sep,
                    os.path.basename(p)
                )

                accsyn_job_data['tasks'].append({
                    'source':'{}{}'.format(source_party, p),
                    'destination':'{}:{}'.format(
                        intermediate_path, destination_path)
                })
                # If server is on prem and serving file system file(s) resides 
                # on, replace destination with:
                # 
                # 'destination':'{}'.format(destination_path)

                file_count += 1

            info('Submitting transfer job to accsyn, {} file(s)/'
                'directories(s)...'.format(file_count))
            self.logger.debug('accsyn JSON submit data: {}'.format(accsyn_job_data))

            j = accsyn_session.create('Job', accsyn_job_data)
            
            info('Submitted (id: {}), check accsyn app for progress'.format(
                j['id']))

        except Exception as e:
            warning(traceback.format_exc())
            error('accsyn distribute CRASHED! Details: {}'.format(str(e)))
            job_final_status = 'failed'
        finally:
            # This will notify the user in the web ui.
            job['status'] = job_final_status
            session.commit()

        return component_count


if __name__ == '__main__':

    if len(ACCSYN_CLIENT_ID)==0:
        raise Exception('No accsyn client id provided!')

    # To be run as standalone code.
    
    arda = AccsynReviewDistributeAction()
    arda.register()

    # Wait for events
    logging.info(
        'Registered actions and listening for events. Use Ctrl-C to abort.'
    )

    arda.session.event_hub.connect()
    arda.session.event_hub.wait()




