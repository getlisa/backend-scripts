#!/usr/bin/env node

// GPT Processing Script - AI Extraction Only
// Processes calls from database that need AI extraction
const { createClient } = require('@supabase/supabase-js');

// Configuration
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://xlfebnscsbakduedrijj.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

console.log('🤖 Starting GPT Processing Script...');
console.log(`🔗 Database: ${SUPABASE_URL}`);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Helper: filter fields
function filterFields(obj, allowedFields) {
  const filtered = {};
  for (const key of allowedFields) {
    if (obj[key] !== undefined) filtered[key] = obj[key];
  }
  return filtered;
}

// Helper: normalize date fields
function normalizeDateFields(obj, dateFields) {
  const out = { ...obj };
  for (const field of dateFields) {
    if (out[field] !== undefined && out[field] !== null) {
      if (typeof out[field] === 'string') {
        const dateStr = out[field].trim();
        if (dateStr === '' || dateStr === 'null' || dateStr === 'undefined' ||
            dateStr === 'unknown' || dateStr === 'Not specified' ||
            dateStr.toLowerCase().includes('morning') ||
            dateStr.toLowerCase().includes('afternoon') ||
            dateStr.toLowerCase().includes('evening') ||
            dateStr.toLowerCase().includes('next week') ||
            dateStr.toLowerCase().includes('to be confirmed')) {
          out[field] = null;
          continue;
        }
        if (/^\d{10,}$/.test(dateStr)) {
          const num = Number(dateStr);
          if (!isNaN(num)) {
            if (field === 'appointment_date') {
              out[field] = new Date(num).toISOString().slice(0, 10);
            } else if (field === 'appointment_time' || field === 'appointment_start' || field === 'appointment_end') {
              out[field] = new Date(num).toISOString().slice(11, 19);
            } else {
              out[field] = new Date(num).toISOString();
            }
          }
        } else if (field === 'appointment_date') {
          try {
            const date = new Date(dateStr);
            if (!isNaN(date.getTime())) {
              out[field] = date.toISOString().slice(0, 10);
            } else {
              out[field] = null;
            }
          } catch (e) {
            out[field] = null;
          }
        } else if (field === 'appointment_time' || field === 'appointment_start' || field === 'appointment_end') {
          try {
            if (/^\d{1,2}:\d{2}(:\d{2})?$/.test(dateStr)) {
              out[field] = dateStr;
            } else {
              out[field] = null;
            }
          } catch (e) {
            out[field] = null;
          }
        }
      } else if (typeof out[field] === 'number') {
        if (field === 'appointment_date') {
          out[field] = new Date(out[field]).toISOString().slice(0, 10);
        } else if (field === 'appointment_time' || field === 'appointment_start' || field === 'appointment_end') {
          out[field] = new Date(out[field]).toISOString().slice(11, 19);
        } else {
          out[field] = new Date(out[field]).toISOString();
        }
      }
    }
  }
  return out;
}

// AI extraction function (matching Cloudflare worker)
async function extractContactInfoAndSummaries(transcript) {
  const prompt = `Extract contact information and generate summaries from this call transcript. Return a JSON object with these fields:
{
  "client_name": "<name or null>",
  "client_email": "<email or null>", 
  "client_address": "<address or null>",
  "appointment_date": "<YYYY-MM-DD or null>",
  "appointment_time": "<HH:MM or null>",
  "summary": "<call summary>",
  "quick_summary": "<1-2 sentence summary>",
  "intent_category": "<Service | Emergency | Quotation | Inquiry | Others>",
  "job_description": "<job description or null>",
  "job_type": "<job type or null>",
  "appointment_date": "<YYYY-MM-DD or null>",
  "appointment_start": "<time or null>",
  "appointment_end": "<time or null>"
}

Transcript: """${transcript}"""`;

  try {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: 'gpt-4',
        messages: [
          { role: 'system', content: 'You are an intelligent assistant for a call receptionist system that extracts structured information from call transcripts.' },
          { role: 'user', content: prompt }
        ]
      }),
    });
    
    const data = await response.json();
    const content = data.choices?.[0]?.message?.content;
    
    try {
      return JSON.parse(content);
    } catch (e) {
      console.error('❌ Failed to parse AI extraction result');
      return {};
    }
  } catch (error) {
    console.error('❌ AI extraction failed:', error.message);
    return {};
  }
}

// Get calls that need GPT processing
async function getCallsForGPTProcessing() {
  console.log('📋 Fetching calls that need GPT processing...');
  
  try {
    const { data: calls, error } = await supabase
      .from('call_logs')
      .select('*')
      .eq('gpt_status', 0)
      .eq('call_status', 'ended')
      .not('transcript', 'is', null)
      .limit(50); // Process up to 50 calls at a time
    
    if (error) {
      console.error('❌ Error fetching calls for GPT processing:', error.message);
      return [];
    }
    
    console.log(`✅ Found ${calls.length} calls that need GPT processing (gpt_status=0, call_status='ended')`);
    return calls || [];
  } catch (error) {
    console.error('❌ Error fetching calls for GPT processing:', error.message);
    return [];
  }
}

// Update gpt_status to 1 (processing)
async function markCallsAsProcessing(callIds) {
  console.log(`🔄 Marking ${callIds.length} calls as processing (gpt_status=1)...`);
  
  try {
    const { error } = await supabase
      .from('call_logs')
      .update({ gpt_status: 1, updated_at: new Date().toISOString() })
      .in('call_id', callIds);
    
    if (error) {
      console.error('❌ Error marking calls as processing:', error.message);
      return false;
    }
    
    console.log(`✅ Successfully marked ${callIds.length} calls as processing`);
    return true;
  } catch (error) {
    console.error('❌ Error marking calls as processing:', error.message);
    return false;
  }
}

// Update gpt_status after processing
async function updateGPTStatus(callId, success) {
  const status = success ? 2 : -1;
  const statusText = success ? 'completed' : 'failed';
  
  try {
    const { error } = await supabase
      .from('call_logs')
      .update({ 
        gpt_status: status, 
        updated_at: new Date().toISOString() 
      })
      .eq('call_id', callId);
    
    if (error) {
      console.error(`❌ Error updating GPT status for call ${callId}:`, error.message);
      return false;
    }
    
    console.log(`✅ Call ${callId} marked as ${statusText} (gpt_status=${status})`);
    return true;
  } catch (error) {
    console.error(`❌ Error updating GPT status for call ${callId}:`, error.message);
    return false;
  }
}

// Process calls with GPT
async function processCallsWithGPT(calls) {
  const processedCalls = [];
  const batchSize = 5;
  
  for (let i = 0; i < calls.length; i += batchSize) {
    const batch = calls.slice(i, i + batchSize);
    console.log(`🤖 Processing GPT batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(calls.length/batchSize)} (${batch.length} calls)`);
    
    const batchPromises = batch.map(async (call) => {
      let extracted = {};
      let success = false;
      
      // AI extraction for calls with transcript
      if (call.transcript && call.transcript.length > 50) {
        try {
          console.log(`🧠 Processing GPT for call ${call.call_id}...`);
          extracted = await extractContactInfoAndSummaries(call.transcript);
          console.log(`✅ GPT extraction completed for call ${call.call_id}`);
          success = true;
        } catch (error) {
          console.error(`❌ GPT extraction failed for call ${call.call_id}:`, error.message);
          success = false;
        }
      } else {
        console.log(`⏭️  Skipping GPT extraction for call ${call.call_id} (no transcript or too short)`);
        success = true; // Consider it successful if no transcript
      }

      // Only update fields that are currently null
      const updateData = {
        updated_at: new Date().toISOString()
      };
      
      // Only update if current value is null
      if (call.appointment_date === null && extracted.appointment_date) {
        updateData.appointment_date = extracted.appointment_date;
      }
      if (call.appointment_time === null && extracted.appointment_start) {
        updateData.appointment_time = extracted.appointment_start;
      }
      if (call.client_name === null && extracted.client_name) {
        updateData.client_name = extracted.client_name;
      }
      if (call.client_address === null && extracted.client_address) {
        updateData.client_address = extracted.client_address;
      }
      if (call.client_email === null && extracted.client_email) {
        updateData.client_email = extracted.client_email;
      }
      if (call.intent === null && extracted.intent_category) {
        updateData.intent = extracted.intent_category;
      }
      if (call.summary === null && extracted.summary) {
        updateData.summary = extracted.summary;
      }
      if (call.quick_summary === null && extracted.quick_summary) {
        updateData.quick_summary = extracted.quick_summary;
      }
      if (call.job_description === null && extracted.job_description) {
        updateData.job_description = extracted.job_description;
      }
      if (call.job_type === null && extracted.job_type) {
        updateData.job_type = extracted.job_type;
      }
      if (call.appointment_start === null && extracted.appointment_start) {
        updateData.appointment_start = extracted.appointment_start;
      }
      if (call.appointment_end === null && extracted.appointment_end) {
        updateData.appointment_end = extracted.appointment_end;
      }
      
      // Update lead_type based on intent
      if (call.lead_type === null && extracted.intent_category) {
        const leadType = extracted.intent_category === 'Service' || 
                        extracted.intent_category === 'Emergency' || 
                        extracted.intent_category === 'Quotation' ? 
                        extracted.intent_category : null;
        if (leadType) {
          updateData.lead_type = leadType;
        }
      }
      
      return {
        call_id: call.call_id,
        updateData,
        success
      };
    });

    const batchResults = await Promise.all(batchPromises);
    processedCalls.push(...batchResults);
  }

  return processedCalls;
}

// Update calls in database
async function updateCallsInDatabase(processedResults) {
  console.log(`💾 Updating ${processedResults.length} calls in database...`);
  
  if (processedResults.length === 0) {
    return { success: 0, failed: 0 };
  }

  let successCount = 0;
  let failedCount = 0;

  // Update each call individually
  for (const result of processedResults) {
    try {
      // Only update if there's data to update
      if (Object.keys(result.updateData).length > 1) { // More than just updated_at
        const { error } = await supabase
          .from('call_logs')
          .update(result.updateData)
          .eq('call_id', result.call_id);

        if (error) {
          console.error(`❌ Error updating call ${result.call_id}:`, error.message);
          failedCount++;
        } else {
          console.log(`✅ Successfully updated call ${result.call_id}`);
          successCount++;
        }
      } else {
        console.log(`⏭️  No updates needed for call ${result.call_id} (all fields already have values)`);
        successCount++;
      }
      
      // Update GPT status
      await updateGPTStatus(result.call_id, result.success);
      
    } catch (error) {
      console.error(`❌ Error processing call ${result.call_id}:`, error.message);
      failedCount++;
      await updateGPTStatus(result.call_id, false);
    }
  }

  console.log(`✅ Database update complete: ${successCount} successful, ${failedCount} failed`);
  return { success: successCount, failed: failedCount };
}

// Main GPT processing function
async function processGPTExtraction() {
  console.log('🤖 Starting GPT processing for calls...');
  
  try {
    // Get calls that need GPT processing
    const calls = await getCallsForGPTProcessing();
    
    if (calls.length === 0) {
      console.log('✅ No calls need GPT processing');
      return;
    }
    
    // Mark calls as processing (gpt_status = 1)
    const callIds = calls.map(call => call.call_id);
    const marked = await markCallsAsProcessing(callIds);
    
    if (!marked) {
      console.error('❌ Failed to mark calls as processing, aborting');
      return;
    }
    
    // Process calls with GPT
    const processedResults = await processCallsWithGPT(calls);
    
    // Update database
    const result = await updateCallsInDatabase(processedResults);
    
    console.log('\n📊 GPT PROCESSING COMPLETE');
    console.log('='.repeat(50));
    console.log(`📞 Total calls processed: ${calls.length}`);
    console.log(`✅ Total successful: ${result.success}`);
    console.log(`❌ Total failed: ${result.failed}`);
    console.log('\n📋 Status Summary:');
    console.log(`  - gpt_status = 0: Pending processing`);
    console.log(`  - gpt_status = 1: Currently processing`);
    console.log(`  - gpt_status = 2: Successfully completed`);
    console.log(`  - gpt_status = -1: Failed processing`);
    
  } catch (error) {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
  }
}

// Run the script
if (require.main === module) {
  processGPTExtraction()
    .then(() => {
      console.log('\n🎉 GPT processing completed successfully!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('\n💥 GPT processing failed:', error.message);
      process.exit(1);
    });
}

module.exports = { processGPTExtraction };
