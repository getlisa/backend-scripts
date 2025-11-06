#!/usr/bin/env node
// Load environment variables
import dotenv from 'dotenv';
dotenv.config();

import { createClient } from '@supabase/supabase-js';
const fetch = (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args));

const ZT_API_BASE = process.env.ZT_API_BASE;
const OB_APP_URL = process.env.OB_APP_URL;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY ;


const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

console.log('🔧 ZT Integration Script loaded');
console.log('🔍 Environment Variables Check:');
console.log('  ZT_API_BASE:', ZT_API_BASE || '❌ NOT LOADED');
console.log('  OB_APP_URL:', OB_APP_URL || '❌ NOT LOADED');
console.log('  SUPABASE_URL:', SUPABASE_URL ? '✅ Loaded' : '❌ NOT LOADED');
console.log('  SUPABASE_SERVICE_ROLE_KEY:', SUPABASE_SERVICE_ROLE_KEY ? '✅ Loaded' : '❌ NOT LOADED');

function getTimestamp() { return Date.now(); }

async function getZTCredentialsForUser(targetUserId) {
  if (!targetUserId) throw new Error('getZTCredentialsForUser: user_id is required');
  const { data, error } = await supabase
    .from('zentrades_tokens')
    .select('username, password, user_id')
    .eq('user_id', targetUserId)
    .single();
  if (error) throw new Error(`zentrades_tokens lookup failed: ${error.message}`);
  if (!data) throw new Error(`No zentrades_tokens row for user_id=${targetUserId}`);
  return {
    credentials: { username: data.username, password: data.password, rememberMe: true },
    user_id: data.user_id,
  };
}

async function getZTTokenForUser(user_id) {
  const { credentials } = await getZTCredentialsForUser(user_id);
  const loginUrl = `${ZT_API_BASE}/api/auth/login?timestamp=${getTimestamp()}`;
  const res = await fetch(loginUrl, {
    method: 'POST',
    headers: { 'accept': 'application/json, text/plain, */*','content-type': 'application/json','origin': 'https://demo-app.zentrades.pro','referer': 'https://demo-app.zentrades.pro/' },
    body: JSON.stringify(credentials)
  });
  if (!res.ok) throw new Error(`Login failed: ${res.status}`);
  const json = await res.json();
  const token = json?.result?.['access-token'];
  if (!token) throw new Error('No access-token');
  return { token, user_id };
}

async function getAgentIdForUser(userId) {
  const { data, error } = await supabase.from('user_profiles').select('agent_id').eq('user_id', userId).single();
  if (error) throw new Error(error.message);
  if (!data?.agent_id) throw new Error('No agent_id for user');
  return data.agent_id;
}

function parseAddress(address) {
  if (!address || address === 'null' || address === 'Not mentioned') {
    return { addressLineOne: 'Address not provided', city: 'Unknown City', state: 'Unknown State', country: 'US', zipCode: '00000' };
  }
  
  // Enhanced address parsing with zip code extraction
  const parts = address.split(',').map(p => p.trim());
  
  // Look for 6-digit zip code in any part of the address
  let zipCode = '00000';
  const zipRegex = /\b\d{6}\b/;
  
  for (const part of parts) {
    const zipMatch = part.match(zipRegex);
    if (zipMatch) {
      zipCode = zipMatch[0];
      break;
    }
  }
  
  // Also check the full address string for zip code
  if (zipCode === '00000') {
    const fullZipMatch = address.match(zipRegex);
    if (fullZipMatch) {
      zipCode = fullZipMatch[0];
    }
  }
  
  return {
    addressLineOne: parts[0] || 'Address not provided',
    city: parts[1] || 'Unknown City',
    state: parts[2] || 'Unknown State',
    country: 'US',
    zipCode: zipCode
  };
}

function formatPhoneNumber(phone) {
  if (!phone) return '(555) 555-5555';
  const digits = phone.replace(/\D/g,'');
  if (digits.length===11 && digits.startsWith('1')) { return `(${digits.slice(1,4)}) ${digits.slice(4,7)}-${digits.slice(7,11)}`; }
  if (digits.length===10) { return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6,10)}`; }
  return '(555) 555-5555';
}

function cleanEmail(email) {
  if (!email) return null;
  const cleaned = email.replace(/\s+/g,'').trim();
  const re = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return re.test(cleaned) ? cleaned : null;
}

function transformLeadToZTBooking(lead) {
  const addressParts = parseAddress(lead.client_address);
  const currentDate = new Date().toISOString().split('T')[0];
  let startBookingTime, endBookingTime, bookDate;
  if (lead.appointment_start && lead.appointment_date) {
    startBookingTime = `${lead.appointment_date}T${lead.appointment_start}.000Z`;
    const start = new Date(`${lead.appointment_date}T${lead.appointment_start}`);
    const end = new Date(start.getTime() + 60 * 60 * 1000);
    endBookingTime = end.toISOString().split('.')[0] + '.000Z';
    bookDate = `${lead.appointment_date}T${lead.appointment_start}.000Z`;
  } else if (lead.appointment_date && lead.appointment_date !== 'null') {
    startBookingTime = `${lead.appointment_date}T02:30:00.000Z`;
    endBookingTime = `${lead.appointment_date}T03:30:00.000Z`;
    bookDate = `${lead.appointment_date}T02:30:00.000Z`;
  } else {
    startBookingTime = `${currentDate}T02:30:00.000Z`;
    endBookingTime = `${currentDate}T03:30:00.000Z`;
    bookDate = `${currentDate}T02:30:00.000Z`;
  }
  const validName = lead.client_name;
  const validEmail = cleanEmail(lead.client_email);
  
  // Email is required - should not reach here if validation passed
  if (!validEmail) {
    throw new Error('Email is required for ZT booking');
  }
  
  const formattedPhone = formatPhoneNumber(lead.from_number);
  return { startBookingTime, endBookingTime, bookDate, name: validName, email: validEmail, phoneNumber: formattedPhone,
    addressLineOne: addressParts.addressLineOne, addressLineTwo: ' ', city: addressParts.city, state: addressParts.state, country: addressParts.country, zipCode: addressParts.zipCode, companyId: 3, description: lead.job_description, source: 'Web' };
}

function validateLeadData(lead) {
  const errors = [];
  
  // Email validation - REQUIRED
  if (!lead.client_email || 
      lead.client_email === 'null' || 
      lead.client_email === 'Not mentioned' || 
      lead.client_email.trim() === '' ||
      !cleanEmail(lead.client_email)) {
    errors.push('Missing or invalid email address');
  }
  
  if (!lead.from_number || lead.from_number === 'null' || lead.from_number.trim()==='') errors.push('Missing phone number');
  if (!lead.client_address || lead.client_address === 'null' || lead.client_address === 'Not mentioned' || lead.client_address.trim()==='') errors.push('Missing address');
  if (lead.appointment_start && lead.appointment_end && lead.appointment_date) {
    const start = new Date(`${lead.appointment_date}T${lead.appointment_start}`);
    const end = new Date(`${lead.appointment_date}T${lead.appointment_end}`);
    const now = new Date();
    if (start <= now) errors.push('Appointment start time must be in the future');
    if (end <= start) errors.push('Appointment end time must be after start time');
  } else if (lead.appointment_date && lead.appointment_date !== 'null') {
    const date = new Date(lead.appointment_date);
    const now = new Date();
    if (date <= now) errors.push('Appointment date must be in the future');
  }
  return { isValid: errors.length===0, errors };
}

async function createZTBooking(lead, token) {
  const payload = transformLeadToZTBooking(lead);
  const res = await fetch(`${ZT_API_BASE}/api/ob/obr/book/`, { method:'POST', headers:{ 'accept':'application/json, text/plain, */*','content-type':'application/json','origin':OB_APP_URL,'referer':OB_APP_URL,'timezone-offset':'-330','user-agent':'Mozilla/5.0','Authorization':`Bearer ${token}` }, body: JSON.stringify(payload) });
  const json = await res.json();
  if (res.ok) return { success:true, booking_id: json?.result?.id || json?.id, response: json };
  return { success:false, error: json?.message || 'Unknown error', response: json };
}

async function logSyncAttempt(callLogId, status, ztBookingId=null, errorMessage=null) {
  const normalizedError = status === 'failed' ? (errorMessage || 'Unknown error') : null;
  const { data: existing } = await supabase.from('zt_sync_logs').select('id').eq('call_log_id', callLogId).maybeSingle();
  if (existing) {
    await supabase
      .from('zt_sync_logs')
      .update({ zt_booking_id: ztBookingId ?? null, sync_status: status, error_message: normalizedError, sync_date: new Date().toISOString(), updated_at: new Date().toISOString() })
      .eq('call_log_id', callLogId);
  } else {
    await supabase
      .from('zt_sync_logs')
      .insert({ call_log_id: callLogId, zt_booking_id: ztBookingId ?? null, sync_status: status, error_message: normalizedError, sync_date: new Date().toISOString() });
  }
}

// Process pending manual items from zt_manual_sync
async function runManualQueue() {
  console.log('🔍 Checking for pending manual sync items...');
  // Fetch pending manual call_ids
  const { data: rows, error } = await supabase.from('zt_manual_sync').select('call_id').eq('status','pending').order('created_at', { ascending: true }).limit(10);
  if (error) throw new Error(error.message);
  if (!rows || rows.length===0) { console.log('✅ No manual items to process'); return; }
  console.log(`📋 Found ${rows.length} pending manual sync items`);
  for (const row of rows) {
    const callId = row.call_id;
    try {
      const { data: lead, error: leadErr } = await supabase.from('call_logs').select('*').eq('call_id', callId).eq('call_status', 'ended').single();
      if (leadErr || !lead) throw new Error(`Lead not found: ${callId}`);
      // Resolve user_id from agent_id
      const { data: userProfile, error: profErr } = await supabase
        .from('user_profiles')
        .select('user_id')
        .eq('agent_id', lead.agent_id)
        .single();
      if (profErr || !userProfile?.user_id) throw new Error(`No user_id mapped for agent_id=${lead.agent_id}`);
      const resolvedUserId = userProfile.user_id;
      // Validate mapping consistency
      const mappedAgentId = await getAgentIdForUser(resolvedUserId);
      if (lead.agent_id !== mappedAgentId) throw new Error(`Agent mismatch for ${callId} (lead.agent_id=${lead.agent_id}, mapped=${mappedAgentId})`);

      // Get ZT token for this specific user
      const { token } = await getZTTokenForUser(resolvedUserId);
      const validation = validateLeadData(lead);
      if (!validation.isValid) { await logSyncAttempt(callId, 'failed', null, `Validation failed: ${validation.errors.join(', ')}`); await supabase.from('zt_manual_sync').update({ status:'failed', error_message: validation.errors.join(', '), updated_at: new Date().toISOString() }).eq('call_id', callId); continue; }
      await logSyncAttempt(callId, 'pending');
      const result = await createZTBooking(lead, token);
      if (result.success) {
        await logSyncAttempt(callId, 'success', result.booking_id);
        await supabase.from('zt_manual_sync').update({ status:'success', zt_booking_id: result.booking_id, error_message: null, updated_at: new Date().toISOString() }).eq('call_id', callId);
        console.log(`Manual synced: ${callId}`);
      } else {
        await logSyncAttempt(callId, 'failed', null, result.error);
        await supabase.from('zt_manual_sync').update({ status:'failed', error_message: result.error, updated_at: new Date().toISOString() }).eq('call_id', callId);
        console.log(`Manual failed: ${callId}`);
      }
    } catch (e) {
      await logSyncAttempt(callId, 'failed', null, e.message);
      await supabase.from('zt_manual_sync').update({ status:'failed', error_message: e.message, updated_at: new Date().toISOString() }).eq('call_id', callId);
      console.log(`Manual error: ${callId}: ${e.message}`);
    }
  }
}

async function main() {
  console.log('🚀 Starting ZT Integration Script...');
  console.log('🔗 Database URL:', process.env.SUPABASE_URL);
  console.log('🔑 Service Key:', process.env.SUPABASE_SERVICE_ROLE_KEY ? 'Present' : 'Missing');
  await runManualQueue();
  console.log('✅ ZT Integration Script completed');
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(err=>{ console.error(err); process.exit(1); });
}
