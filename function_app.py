import azure.functions as func
import logging
import aiohttp
import asyncio
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any
from azure.storage.blob import BlobServiceClient, ContentSettings

# Configure logging for production
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Configuration from environment variables (following Azure best practices)
AZURE_STORAGE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
if not AZURE_STORAGE_CONNECTION_STRING:
    logging.error("❌ AZURE_STORAGE_CONNECTION_STRING environment variable not set")
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is required")

# Container Configuration
INPUT_CONTAINER = os.environ.get('INPUT_CONTAINER', 'stampedstorage')
CLASSIFICATION_CONTAINER = os.environ.get('CLASSIFICATION_CONTAINER', 'classificationstorage')
RESULTS_CONTAINER = os.environ.get('RESULTS_CONTAINER', 'resultstorage')

# Classification API Configuration
CLASSIFICATION_API_URL = os.environ.get('CLASSIFICATION_API_URL')
CLASSIFICATION_API_CODE = os.environ.get('CLASSIFICATION_API_CODE')
CLASSIFICATION_API_TIMEOUT = int(os.environ.get('CLASSIFICATION_API_TIMEOUT', '300'))

if not CLASSIFICATION_API_URL or not CLASSIFICATION_API_CODE:
    logging.error("❌ Classification API configuration missing from environment variables")
    raise ValueError("CLASSIFICATION_API_URL and CLASSIFICATION_API_CODE environment variables are required")

def get_blob_service_client() -> BlobServiceClient:
    """Get Azure Blob Service Client with error handling"""
    try:
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError("Azure Storage connection string not configured")
        
        return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    except Exception as e:
        logging.error(f"❌ Failed to create blob service client: {str(e)}")
        raise

app = func.FunctionApp()

@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')
    
    # Always log that function was triggered
    logging.info('🚀 Event Grid function triggered!')
    
    try:
        # Log raw event for debugging
        logging.info(f'🔍 Raw event ID: {azeventgrid.id if hasattr(azeventgrid, "id") else "No ID"}')
        logging.info(f'🔍 Raw event type: {azeventgrid.event_type if hasattr(azeventgrid, "event_type") else "No event type"}')
        logging.info(f'🔍 Raw event subject: {azeventgrid.subject if hasattr(azeventgrid, "subject") else "No subject"}')
        
        # Handle EventGrid subscription validation
        if hasattr(azeventgrid, 'event_type') and azeventgrid.event_type == 'Microsoft.EventGrid.SubscriptionValidationEvent':
            logging.info('🔐 Handling EventGrid subscription validation')
            try:
                event_data = azeventgrid.get_json()
                validation_code = event_data.get('validationCode')
                if validation_code:
                    logging.info(f'✅ Validation successful with code: {validation_code}')
                    # For EventGrid trigger, we just need to process the validation successfully
                    return
                else:
                    logging.error('❌ No validation code in subscription validation event')
            except Exception as e:
                logging.error(f'❌ Error handling validation event: {str(e)}')
            return
        
        # Validate EventGrid event structure
        if not hasattr(azeventgrid, 'event_type') or not azeventgrid.event_type:
            logging.error('❌ Invalid EventGrid event: Missing event_type')
            return
            
        if not hasattr(azeventgrid, 'get_json'):
            logging.error('❌ Invalid EventGrid event: Cannot get JSON data')
            return
        
        # Parse Event Grid event data
        try:
            event_data = azeventgrid.get_json()
            logging.info(f'📧 Event type: {azeventgrid.event_type}')
            logging.info(f'📧 Event subject: {azeventgrid.subject}')
            logging.info(f'📧 Event data keys: {list(event_data.keys()) if event_data else "No data"}')
        except Exception as e:
            logging.error(f'❌ Failed to parse event JSON: {str(e)}')
            return
        
        # Check if this is a blob created event
        if azeventgrid.event_type != 'Microsoft.Storage.BlobCreated':
            logging.info(f'⏭️ Skipping non-blob-created event: {azeventgrid.event_type}')
            return
            
        # Extract blob information from event data
        blob_url = event_data.get('url', '')
        api_data = event_data.get('api', '')
        container_name = ""
        blob_name = ""
        
        # Parse blob URL to extract container and blob name
        # URL format: https://accountname.blob.core.windows.net/container/blobname
        if blob_url:
            try:
                # Extract container and blob name from URL
                url_parts = blob_url.split('/')
                if len(url_parts) >= 5:
                    container_name = url_parts[3]  # Container name
                    blob_name = '/'.join(url_parts[4:])  # Blob name (can have folders)
                    
                logging.info(f'📁 Container: {container_name}')
                logging.info(f'📄 Blob name: {blob_name}')
                logging.info(f'🔗 Full URL: {blob_url}')
            except Exception as e:
                logging.error(f'❌ Error parsing blob URL: {str(e)}')
                return
        
        # Alternative: Extract from subject if URL parsing fails
        if not blob_name and azeventgrid.subject:
            # Subject format: /blobServices/default/containers/containername/blobs/blobname
            subject_parts = azeventgrid.subject.split('/')
            if 'containers' in subject_parts and 'blobs' in subject_parts:
                try:
                    container_idx = subject_parts.index('containers') + 1
                    blob_idx = subject_parts.index('blobs') + 1
                    if container_idx < len(subject_parts):
                        container_name = subject_parts[container_idx]
                    if blob_idx < len(subject_parts):
                        blob_name = '/'.join(subject_parts[blob_idx:])
                    
                    logging.info(f'📁 Container (from subject): {container_name}')
                    logging.info(f'📄 Blob name (from subject): {blob_name}')
                except Exception as e:
                    logging.error(f'❌ Error parsing subject: {str(e)}')
                    return
        
        if not blob_name:
            logging.error(f'❌ Could not extract blob name from event')
            return
            
        file_name = blob_name.split('/')[-1] if '/' in blob_name else blob_name
        logging.info(f'📄 File name: {file_name}')
        
        # Check if the file is a PDF
        if not file_name.lower().endswith('.pdf'):
            logging.info(f'⏭️ Skipping non-PDF file: {file_name}')
            return
            
        # Check if it's already a classified file (avoid infinite loop)
        if '_classified' in file_name.lower():
            logging.info(f'⏭️ Skipping already classified file: {file_name}')
            return
        
        # Check if blob is in the correct container
        if container_name != INPUT_CONTAINER:
            logging.info(f'⏭️ Skipping file not in {INPUT_CONTAINER} container. Found in: {container_name}')
            return
            
        logging.info(f'✅ Processing PDF: {file_name} from container: {container_name}')
        
        # Download blob content
        pdf_content = download_blob_content(container_name, blob_name)
        
        if not pdf_content:
            logging.error(f'❌ Failed to download blob: {blob_name}')
            return
            
        # Run async processing
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            result = loop.run_until_complete(
                asyncio.wait_for(
                    process_pdf_classification(pdf_content, file_name, blob_url),
                    timeout=600.0
                )
            )
            
            if result['success']:
                logging.info(f'✅ Successfully processed: {file_name} -> {result.get("classification", "unknown")}')
                logging.info('🎉 Function execution completed successfully')
            else:
                logging.error(f'❌ Failed to process: {file_name} - {result.get("error", "unknown")}')
                # Don't raise exception here - log error but let function complete successfully
                
        except asyncio.TimeoutError:
            logging.error(f'⏱️ Processing timeout: {file_name}')
            # Don't raise exception - log error but let function complete successfully
        except Exception as e:
            logging.error(f'🔥 Processing error: {file_name} - {str(e)}')
            # Don't raise exception - log error but let function complete successfully
        finally:
            try:
                loop.close()
            except:
                pass
            
    except Exception as e:
        logging.error(f'💥 Event processing error: {str(e)}')
        logging.error(f'💥 Event data: {azeventgrid.get_json() if hasattr(azeventgrid, "get_json") else "No event data"}')
        # Don't re-raise the exception - EventGrid considers any exception as a delivery failure
        
    # Always log function completion
    logging.info('🏁 Function execution completed')

def download_blob_content(container_name: str, blob_name: str) -> Optional[bytes]:
    """Download blob content from storage"""
    try:
        blob_service_client = get_blob_service_client()
        
        logging.info(f'📥 Downloading blob: {blob_name} from container: {container_name}')
        
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        
        # Check if blob exists
        if not blob_client.exists():
            logging.error(f'❌ Blob does not exist: {blob_name}')
            return None
        
        # Download blob content
        blob_data = blob_client.download_blob()
        content = blob_data.readall()
        
        logging.info(f'✅ Successfully downloaded blob: {blob_name} ({len(content)} bytes)')
        return content
        
    except Exception as e:
        logging.error(f'❌ Error downloading blob {blob_name} from {container_name}: {str(e)}')
        return None

async def process_pdf_classification(pdf_content: bytes, file_name: str, blob_uri: str) -> dict:
    """Process PDF through classification API and store results"""
    try:
        # Call classification API
        classification_result = await call_classification_api(pdf_content, file_name)
        if not classification_result:
            return {'success': False, 'error': 'API call failed'}
        
        # Upload classified PDF
        upload_result = await upload_classified_pdf(pdf_content, file_name, classification_result)
        if not upload_result['success']:
            return upload_result
        
        # Save JSON result
        json_result = await save_classification_json(file_name, classification_result)
        
        return {
            'success': True,
            'classified_filename': upload_result['classified_filename'],
            'json_filename': json_result.get('json_filename') if json_result['success'] else None,
            'classification': classification_result.get('classification', 'unknown'),
            'json_stored': json_result['success']
        }
        
    except Exception as e:
        logging.error(f'Error processing PDF: {str(e)}')
        return {'success': False, 'error': str(e)}

async def call_classification_api(pdf_content: bytes, file_name: str) -> Optional[Dict[str, Any]]:
    """Call the classification API"""
    try:
        api_url = f"{CLASSIFICATION_API_URL}?code={CLASSIFICATION_API_CODE}"
        
        logging.info(f'🌐 Calling classification API for: {file_name}')
        logging.info(f'🌐 API URL: {CLASSIFICATION_API_URL}')
        
        # Prepare form data
        data = aiohttp.FormData()
        data.add_field('file', pdf_content, filename=file_name, content_type='application/pdf')
        
        timeout = aiohttp.ClientTimeout(total=CLASSIFICATION_API_TIMEOUT)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(api_url, data=data) as response:
                logging.info(f'📊 API Response Status: {response.status}')
                
                if response.status == 200:
                    result = await handle_classification_response(response)
                    if result:
                        logging.info(f'✅ API call successful for: {file_name}')
                        logging.info(f'📋 Classification result: {result.get("classification", "unknown")}')
                    return result
                else:
                    response_text = await response.text()
                    logging.error(f'❌ API error {response.status}: {response_text}')
                    return None
                    
    except asyncio.TimeoutError:
        logging.error(f'⏱️ API timeout for: {file_name}')
        return None
    except Exception as e:
        logging.error(f'❌ API call error for {file_name}: {str(e)}')
        return None

async def handle_classification_response(response) -> Optional[Dict[str, Any]]:
    """Handle API response"""
    try:
        content_type = response.headers.get('content-type', '')
        
        if 'application/json' in content_type:
            try:
                result = await response.json()
                return result
            except Exception as e:
                logging.warning(f'JSON parse error: {str(e)}')
        
        logging.warning('No valid JSON response received')
        return None
                
    except Exception as e:
        logging.error(f'Response handling error: {str(e)}')
        return None

async def upload_classified_pdf(pdf_content: bytes, original_file_name: str, classification_result: Dict[str, Any]) -> dict:
    """Upload PDF to classification storage"""
    try:
        name_without_ext = os.path.splitext(original_file_name)[0]
        extension = os.path.splitext(original_file_name)[1]
        classification = classification_result.get('classification', 'unknown')
        classified_filename = f"{name_without_ext}_classified_{classification}{extension}"
        
        logging.info(f'📤 Uploading classified PDF: {classified_filename}')
        
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=CLASSIFICATION_CONTAINER,
            blob=classified_filename
        )
        
        # Upload with metadata
        blob_client.upload_blob(
            data=pdf_content,
            content_settings=ContentSettings(content_type='application/pdf'),
            metadata={
                'classification': str(classification),
                'processed_at': datetime.utcnow().isoformat(),
                'original_filename': original_file_name
            },
            overwrite=True
        )
        
        logging.info(f'✅ Successfully uploaded classified PDF: {classified_filename} ({len(pdf_content)} bytes)')
        
        return {
            'success': True,
            'classified_filename': classified_filename,
            'file_size': len(pdf_content)
        }
        
    except Exception as e:
        logging.error(f'❌ Upload error: {str(e)}')
        return {'success': False, 'error': str(e)}

async def save_classification_json(original_file_name: str, classification_result: Dict[str, Any]) -> dict:
    """Save JSON result"""
    try:
        name_without_ext = os.path.splitext(original_file_name)[0]
        json_filename = f"{name_without_ext}_classification_result.json"
        
        logging.info(f'💾 Saving classification JSON: {json_filename}')
        
        # Create JSON content
        json_data = {
            'original_filename': original_file_name,
            'processed_at': datetime.utcnow().isoformat(),
            'classification_result': classification_result,
            'metadata': {
                'version': '1.0',
                'source': 'azure_function_classification'
            }
        }
        
        json_content = json.dumps(json_data, indent=2, ensure_ascii=False)
        json_bytes = json_content.encode('utf-8')
        
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=RESULTS_CONTAINER,
            blob=json_filename
        )
        
        blob_client.upload_blob(
            data=json_bytes,
            content_settings=ContentSettings(content_type='application/json'),
            metadata={
                'content_type': 'application/json',
                'original_filename': original_file_name,
                'processed_at': datetime.utcnow().isoformat()
            },
            overwrite=True
        )
        
        logging.info(f'✅ Successfully saved JSON result: {json_filename} ({len(json_bytes)} bytes)')
        
        return {
            'success': True,
            'json_filename': json_filename,
            'file_size': len(json_bytes)
        }
        
    except Exception as e:
        logging.error(f'❌ JSON save error: {str(e)}')
        return {'success': False, 'error': str(e)}

# Test function for debugging EventGrid integration
def test_event_grid_integration():
    """Test function to validate EventGrid integration"""
    logging.info('🧪 Testing EventGrid integration...')
    
    # Test blob service client
    try:
        blob_service_client = get_blob_service_client()
        logging.info('✅ Blob service client created successfully')
        
        # Test container access
        container_client = blob_service_client.get_container_client(INPUT_CONTAINER)
        if container_client.exists():
            logging.info(f'✅ Input container "{INPUT_CONTAINER}" exists and is accessible')
        else:
            logging.error(f'❌ Input container "{INPUT_CONTAINER}" does not exist or not accessible')
            
    except Exception as e:
        logging.error(f'❌ Blob service client test failed: {str(e)}')
    
    # Test API endpoint
    logging.info(f'🌐 API URL configured: {CLASSIFICATION_API_URL}')
    logging.info('🧪 EventGrid integration test completed')

