#!/usr/bin/env node

// Call Processing Script - Retell AI to Database Only
// Processes all agents from user_profiles, fetches calls from Retell AI, and stores in database
// GPT processing is handled separately by gpt-process.js
const { createClient } = require('@supabase/supabase-js');

// Configuration
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY ;
const RETELL_API_KEY = process.env.RETELL_API_KEY ;

console.log('🚀 Starting Call Processing Script (Retell AI → Database)...');
console.log(`🔗 Database: ${SUPABASE_URL}`);
console.log('📝 Note: GPT processing handled separately by gpt-process.js');

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

// Get all agent IDs from user_profiles
async function getAllAgents() {
  console.log('📋 Fetching all agents from user_profiles...');
  
  try {
    const { data: agents, error } = await supabase
      .from('user_profiles')
      .select('agent_id')
      .not('agent_id', 'is', null);
    
    if (error) {
      console.error('❌ Error fetching agents:', error.message);
      return [];
    }
    
    const agentIds = (agents || []).map(row => row.agent_id).filter(Boolean);
    console.log(`✅ Found ${agentIds.length} agents: ${agentIds.join(', ')}`);
    
    return agentIds;
  } catch (error) {
    console.error('❌ Error fetching agents:', error.message);
    return [];
  }
}

// Fetch calls from RetellAI API for specific agent
async function fetchCallsFromRetellAPI(agentId) {
  console.log(`📞 Fetching calls for agent: ${agentId}`);
  
  const apiUrl = 'https://api.retellai.com/v2/list-calls';
  
  // Calculate 48 hours ago in milliseconds
  const now = Date.now();
  const fortyEightHoursAgo = now - (300 * 60 * 60 * 1000);
  
  const requestBody = {
    filter_criteria: {
      agent_id: [agentId],
      start_timestamp: { 
        lower_threshold: fortyEightHoursAgo 
      }
    },
    limit: 1000,
    sort_order: 'descending'
  };
  
  try {
    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${RETELL_API_KEY}`
      },
      body: JSON.stringify(requestBody)
    });
    
    if (!response.ok) {
      console.error(`❌ RetellAI API request failed with status ${response.status}`);
      return [];
    }
    
    const data = await response.json();
    if (Array.isArray(data)) {
      console.log(`✅ Fetched ${data.length} calls for agent ${agentId}`);
      return data;
    }
    return [];
  } catch (error) {
    console.error(`❌ Error fetching calls from RetellAI for agent ${agentId}:`, error.message);
    return [];
  }
}

// Bulk check existing calls in Supabase
async function bulkCheckExistingCalls(callIds) {
  console.log(`🔍 Checking ${callIds.length} call IDs in database...`);
  
  if (callIds.length === 0) {
    return new Map();
  }

  const { data: existingCalls, error } = await supabase
    .from('call_logs')
    .select('call_id, call_status')
    .in('call_id', callIds);

  if (error) {
    console.error('❌ Error checking existing calls:', error.message);
    return new Map();
  }

  const existingCallsMap = new Map();
  (existingCalls || []).forEach(call => {
    existingCallsMap.set(call.call_id, call);
  });

  console.log(`✅ Found ${existingCallsMap.size} existing calls out of ${callIds.length} checked`);
  return existingCallsMap;
}

// Filter calls that need processing
function filterCallsForProcessing(calls, existingCallsMap) {
  const callsToProcess = calls.filter(call => {
    const existing = existingCallsMap.get(call.call_id);
    if (!existing) {
      return true; // New call, needs processing
    }
    // Skip if already ended or failed
    if (existing.call_status === 'ended' || existing.call_status === 'failed') {
      return false;
    }
    return true; // Needs update
  });

  console.log(`🔄 ${callsToProcess.length} calls need processing out of ${calls.length} total calls`);
  return callsToProcess;
}

// Simple intent classification (no AI)
function classifyIntent(text) {
  const content = text.toLowerCase();
  if (content.includes('emergency') || content.includes('urgent') || content.includes('asap')) {
    return 'Emergency';
  } else if (content.includes('service') || content.includes('repair') || content.includes('fix')) {
    return 'Service';
  } else if (content.includes('quote') || content.includes('estimate') || content.includes('price')) {
    return 'Quotation';
  }
  return 'Inquiry';
}


// Process calls - basic mapping only (no AI)
async function processCalls(calls, agentId) {
  const CALL_LOGS_FIELDS = [
    'call_id', 'agent_id', 'call_status', 'start_timestamp', 'end_timestamp', 'transcript',
    'recording_url', 'call_type', 'from_number', 'appointment_status', 'appointment_date',
    'appointment_time', 'client_name', 'client_address', 'client_email', 'notes',
    'user_sentiment', 'call_successful', 'in_voicemail', 'processed', 'created_at', 'updated_at',
    'intent', 'summary', 'quick_summary', 'lead_type', 'job_description', 'job_type',
    'appointment_start', 'appointment_end', 'manual_notes', 'call_analysis', 'email_sent'
  ];

  const processedCalls = [];
  const batchSize = 10; // Larger batches since no AI processing
  const maxCallsToProcess = Math.min(calls.length, 50); // Process more calls
  const callsToProcessNow = calls.slice(0, maxCallsToProcess);
  
  if (calls.length > maxCallsToProcess) {
    console.log(`📊 Processing ${maxCallsToProcess} calls (out of ${calls.length}) for agent ${agentId}`);
  }
  
  for (let i = 0; i < callsToProcessNow.length; i += batchSize) {
    const batch = callsToProcessNow.slice(i, i + batchSize);
    console.log(`🔄 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(callsToProcessNow.length/batchSize)} (${batch.length} calls)`);
    
    const batchPromises = batch.map(async (call) => {
      // Simple intent classification (no AI)
      const intent = classifyIntent(call.transcript || call.summary || '');
      
      // Extract job_type from call_analysis.custom_analysis_data
      const jobType = call.call_analysis?.custom_analysis_data?.job_type || call.job_type || null;
      
      // Extract client info from collected_dynamic_variables
      const dynamicVars = call.collected_dynamic_variables || {};
      const clientName = dynamicVars.user_name || call.client_name || null;
      const clientAddress = dynamicVars.validated_address || dynamicVars.raw_input || call.client_address || null;
      const clientEmail = dynamicVars.user_email || call.client_email || null;
      
      const mappedCall = {
        call_id: call.call_id,
        agent_id: agentId,
        call_status: call.call_status || null,
        start_timestamp: call.start_timestamp || null,
        end_timestamp: call.end_timestamp || null,
        transcript: call.transcript || null,
        recording_url: call.recording_url || null,
        call_type: call.call_type || null,
        from_number: call.from_number || null,
        appointment_status: call.appointment_status || null,
        appointment_date: call.appointment_date || null,
        appointment_time: call.appointment_time || null,
        client_name: clientName,
        client_address: clientAddress,
        client_email: clientEmail,
        notes: call.notes || null,
        user_sentiment: call.user_sentiment || call.call_analysis?.user_sentiment || null,
        call_successful: call.call_successful ?? call.call_analysis?.call_successful ?? null,
        in_voicemail: call.in_voicemail ?? call.call_analysis?.in_voicemail ?? null,
        processed: call.processed ?? false,
        created_at: call.created_at || call.start_timestamp || new Date().toISOString(),
        updated_at: new Date().toISOString(),
        intent,
        summary: call.summary || call.call_analysis?.call_summary || null,
        quick_summary: call.quick_summary || null,
        lead_type: intent === 'Service' || intent === 'Emergency' || intent === 'Quotation' ? intent : null,
        job_description: call.job_description || null,
        job_type: jobType,
        appointment_start: call.appointment_start || null,
        appointment_end: call.appointment_end || null,
        manual_notes: call.manual_notes || null,
        call_analysis: call.call_analysis || null,
        email_sent: call.email_sent ?? 0
      };
      
      const callLog = filterFields(
        normalizeDateFields(mappedCall, [
          'start_timestamp', 'end_timestamp', 'appointment_date', 'appointment_time',
          'appointment_start', 'appointment_end', 'created_at', 'updated_at'
        ]),
        CALL_LOGS_FIELDS
      );
      
      return callLog;
    });

    const batchResults = await Promise.all(batchPromises);
    processedCalls.push(...batchResults);
  }

  if (calls.length > maxCallsToProcess) {
    console.log(`📝 Note: ${calls.length - maxCallsToProcess} calls remain for future processing`);
  }

  return processedCalls;
}

// Bulk upsert calls to Supabase
async function bulkUpsertCalls(calls) {
  console.log(`💾 Bulk upserting ${calls.length} calls to database...`);
  
  if (calls.length === 0) {
    return { success: 0, failed: 0 };
  }

  const batchSize = 100;
  let successCount = 0;
  let failedCount = 0;

  for (let i = 0; i < calls.length; i += batchSize) {
    const batch = calls.slice(i, i + batchSize);
    console.log(`💾 Upserting batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(calls.length/batchSize)} (${batch.length} calls)`);
    
    const { error } = await supabase
      .from('call_logs')
      .upsert(batch, { onConflict: 'call_id' });

    if (error) {
      console.error(`❌ Error upserting batch:`, error.message);
      failedCount += batch.length;
    } else {
      console.log(`✅ Successfully upserted batch of ${batch.length} calls`);
      successCount += batch.length;
    }
  }

  console.log(`✅ Database update complete: ${successCount} successful, ${failedCount} failed`);
  return { success: successCount, failed: failedCount };
}

// Main processing function
async function processAllAgents() {
  console.log('🚀 Starting call processing for all agents...');
  
  try {
    // Get all agents
    const agentIds = await getAllAgents();
    
    if (agentIds.length === 0) {
      console.log('❌ No agents found to process');
      return;
    }
    
    let totalResults = [];
    
    // Process each agent
    for (const agentId of agentIds) {
      console.log(`\n🎯 Processing agent: ${agentId}`);
      
      try {
        // Fetch calls from RetellAI
        const callsFromRetellAI = await fetchCallsFromRetellAPI(agentId);
        
        if (callsFromRetellAI.length === 0) {
          console.log(`📭 No calls found for agent ${agentId}`);
          totalResults.push({
            agent_id: agentId,
            source: 'retellai',
            total_fetched: 0,
            existing_calls: 0,
            processed: 0,
            success: 0,
            failed: 0
          });
          continue;
        }
        
        
        // Check existing calls
        const callIds = callsFromRetellAI.map(call => call.call_id);
        const existingCallsMap = await bulkCheckExistingCalls(callIds);
        
        // Filter calls that need processing
        const callsToProcess = filterCallsForProcessing(callsFromRetellAI, existingCallsMap);
        
        let upsertResult = { success: 0, failed: 0 };
        
        if (callsToProcess.length === 0) {
          console.log(`✅ All calls for agent ${agentId} are already processed`);
        } else {
          console.log(`🔄 Processing ${callsToProcess.length} new/updated calls for agent ${agentId}`);
          
          // Process calls (AI extraction) in batches
          const processedCalls = await processCalls(callsToProcess, agentId);
          
          // Bulk upsert to Supabase
          upsertResult = await bulkUpsertCalls(processedCalls);
          
          console.log(`✅ Agent ${agentId}: ${upsertResult.success} successful, ${upsertResult.failed} failed`);
        }
        
        totalResults.push({
          agent_id: agentId,
          source: 'retellai',
          total_fetched: callsFromRetellAI.length,
          existing_calls: existingCallsMap.size,
          processed: callsToProcess.length,
          success: upsertResult.success,
          failed: upsertResult.failed
        });
        
      } catch (error) {
        console.error(`❌ Error processing agent ${agentId}:`, error.message);
        totalResults.push({
          agent_id: agentId,
          source: 'retellai',
          error: error.message,
          total_fetched: 0,
          existing_calls: 0,
          processed: 0,
          success: 0,
          failed: 0
        });
      }
    }
    
    // Summary
    console.log('\n📊 PROCESSING COMPLETE');
    console.log('='.repeat(50));
    
    const totalFetched = totalResults.reduce((sum, r) => sum + (r.total_fetched || 0), 0);
    const totalExisting = totalResults.reduce((sum, r) => sum + (r.existing_calls || 0), 0);
    const totalProcessed = totalResults.reduce((sum, r) => sum + (r.processed || 0), 0);
    const totalSuccess = totalResults.reduce((sum, r) => sum + (r.success || 0), 0);
    const totalFailed = totalResults.reduce((sum, r) => sum + (r.failed || 0), 0);
    
    console.log(`📞 Total calls fetched: ${totalFetched}`);
    console.log(`✅ Total existing calls: ${totalExisting}`);
    console.log(`🔄 Total calls processed: ${totalProcessed}`);
    console.log(`✅ Total successful: ${totalSuccess}`);
    console.log(`❌ Total failed: ${totalFailed}`);
    console.log(`🎯 Agents processed: ${agentIds.length}`);
    
    console.log('\n📋 Per-agent results:');
    totalResults.forEach(result => {
      if (result.error) {
        console.log(`❌ ${result.agent_id}: ERROR - ${result.error}`);
      } else {
        console.log(`✅ ${result.agent_id}: ${result.success} success, ${result.failed} failed (${result.processed} processed)`);
      }
    });
    
  } catch (error) {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
  }
}

// Run the script
if (require.main === module) {
  processAllAgents()
    .then(() => {
      console.log('\n🎉 Script completed successfully!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('\n💥 Script failed:', error.message);
      process.exit(1);
    });
}

module.exports = { processAllAgents };
