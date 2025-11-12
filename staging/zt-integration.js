#!/usr/bin/env node
// Load environment variables
const dotenv = require('dotenv');
dotenv.config();

const { createClient } = require('@supabase/supabase-js');
const fetch = (...args) => import('node-fetch').then(({ default: fetchFn }) => fetchFn(...args));
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

// Calculate timezone offset in minutes for a given timezone
function getTimezoneOffsetMinutes(timezoneRegionName) {
  if (!timezoneRegionName) return 0;
  
  try {
    // Create a date in the target timezone
    const date = new Date();
    const utcDate = new Date(date.toLocaleString('en-US', { timeZone: 'UTC' }));
    const tzDate = new Date(date.toLocaleString('en-US', { timeZone: timezoneRegionName }));
    
    // Calculate offset in minutes
    const offsetMinutes = (tzDate.getTime() - utcDate.getTime()) / (1000 * 60);
    return offsetMinutes;
  } catch (error) {
    console.error(`Error calculating timezone offset for ${timezoneRegionName}:`, error.message);
    return 0;
  }
}

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

async function requestZTToken(credentials, userId) {
  if (!credentials?.username || !credentials?.password) throw new Error('requestZTToken: incomplete credentials');
  const loginUrl = `${ZT_API_BASE}/api/auth/login?timestamp=${getTimestamp()}`;
  const res = await fetch(loginUrl, {
    method: 'POST',
    headers: {
      'accept': 'application/json, text/plain, */*',
      'content-type': 'application/json',
      'referer': OB_APP_URL
    },
    body: JSON.stringify(credentials)
  });
  if (!res.ok) {
    const errorBody = await res.text();
    throw new Error(`Login failed: ${res.status} - ${errorBody}`);
  }
  const json = await res.json();
  const token = json?.result?.['access-token'];
  const companyId = json?.result?.user?.company?.id;
  if (!token) throw new Error('No access-token');
  
  // Fetch timezone from company API
  let timezone = null;
  if (companyId) {
    try {
      const companyRes = await fetch(`${ZT_API_BASE}/api/company`, {
        method: 'GET',
        headers: {
          'accept': 'application/json, text/plain, */*',
          'Authorization': `Bearer ${token}`,
          'referer': OB_APP_URL
        }
      });
      if (companyRes.ok) {
        const companyData = await companyRes.json();
        timezone = companyData?.result?.timezoneRegionName;
      }
    } catch (error) {
      console.error('Failed to fetch company timezone:', error.message);
    }
  }
  
  return { token, user_id: userId, company_id: companyId || 3, timezone };
}

async function getZTTokenForUser(user_id) {
  const { credentials } = await getZTCredentialsForUser(user_id);
  return requestZTToken(credentials, user_id);
}

async function getZTTokenForAgent(agentId) {
  if (!agentId) throw new Error('getZTTokenForAgent: agent_id is required');
  const { data: profiles, error: profilesErr } = await supabase
    .from('user_profiles')
    .select('user_id')
    .eq('agent_id', agentId);
  if (profilesErr) throw new Error(`user_profiles lookup failed: ${profilesErr.message}`);
  const userIds = (profiles || [])
    .map(profile => profile?.user_id)
    .filter((id) => Boolean(id));
  if (userIds.length === 0) throw new Error(`No user_ids mapped for agent_id=${agentId}`);

  const { data: tokenRows, error: tokenErr } = await supabase
    .from('zentrades_tokens')
    .select('user_id, username, password')
    .in('user_id', userIds);
  if (tokenErr) throw new Error(`zentrades_tokens lookup failed: ${tokenErr.message}`);
  if (!tokenRows || tokenRows.length === 0) throw new Error(`No zentrades_tokens row for agent_id=${agentId}`);

  const matchingToken = tokenRows.find(row => row?.username && row?.password) || tokenRows[0];
  if (!matchingToken?.user_id) throw new Error(`Invalid zentrades_tokens entry for agent_id=${agentId}`);

  const credentials = { username: matchingToken.username, password: matchingToken.password, rememberMe: true };
  const tokenResult = await requestZTToken(credentials, matchingToken.user_id);
  // company_id comes from the login API response
  return tokenResult;
}

function parseAddress(address) {
  if (!address || address === 'null' || address === 'Not mentioned') {
    return { addressLineOne: 'Address not provided', city: 'Unknown City', state: 'Unknown State', country: 'US', zipCode: '00000' };
  }
  
  // Split by comma
  const parts = address.split(',').map(p => p.trim());
  
  // Extract zip code (5 or 6 digits)
  let zipCode = '00000';
  let state = 'Unknown State';
  let stateZipPart = null;
  let stateZipIndex = -1;
  
  // Find the part containing state/province and postal code
  // US ZIP: 5 digits (12345) or 9 digits (12345-6789)
  // Canada Postal: A1A 1A1 or A1A1A1 or A1A
  const zipRegex = /\b\d{5}(?:-\d{4})?\b|\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b|\b[A-Z]\d[A-Z]\b/i;
  for (let i = 0; i < parts.length; i++) {
    const zipMatch = parts[i].match(zipRegex);
    if (zipMatch) {
      zipCode = zipMatch[0];
      stateZipPart = parts[i];
      stateZipIndex = i;
      
      // Extract state/province (usually 2 letters before the postal code)
      // For US: NY 11746 or NY 10038-9110
      // For Canada: ON A1A 1A1 or ON A1A1A1
      const stateMatch = parts[i].match(/\b([A-Z]{2})\s+(?:\d{5}(?:-\d{4})?|[A-Z]\d[A-Z]\s?\d[A-Z]\d|[A-Z]\d[A-Z])\b/i);
      if (stateMatch) {
        state = stateMatch[1].toUpperCase();
      }
      break;
    }
  }
  
  // Determine address line and city
  let addressLineOne = parts[0] || 'Address not provided';
  let city = 'Unknown City';
  
  if (stateZipIndex > 0) {
    // City is the part before state/zip
    city = parts[stateZipIndex - 1] || 'Unknown City';
  } else if (parts.length > 1) {
    city = parts[1] || 'Unknown City';
  }
  
  // Country is usually the last part - normalize to US or Canada
  let country = 'US'; // Default to US
  if (parts.length > 0) {
    const lastPart = parts[parts.length - 1].toUpperCase();
    if (lastPart === 'CANADA' || lastPart === 'CA' || lastPart === 'CAN') {
      country = 'Canada';
    } else if (lastPart === 'USA' || lastPart === 'US' || lastPart === 'UNITED STATES') {
      country = 'US';
    }
  }
  
  return {
    addressLineOne,
    city,
    state,
    country,
    zipCode
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

function transformLeadToZTBooking(lead, ztCompanyId = 3, timezone = null) {
  const addressParts = parseAddress(lead.client_address);
  const currentDate = new Date().toISOString().split('T')[0];
  let startBookingTime, endBookingTime, bookDate;
  
  // Get timezone offset dynamically based on the company's timezone
  const ZT_TIMEZONE_OFFSET_MINUTES = timezone ? getTimezoneOffsetMinutes(timezone) : 0;
  
  if (lead.appointment_start && lead.appointment_date) {
    // The platform converts UTC time to its timezone for display
    // We need to send UTC time that will display as the original time
    
    // Parse the desired display time as UTC
    const displayTime = `${lead.appointment_date}T${lead.appointment_start}Z`;
    const displayDate = new Date(displayTime);
    
    // Subtract the timezone offset to get the UTC time to send
    const utcTime = new Date(displayDate.getTime() - (ZT_TIMEZONE_OFFSET_MINUTES * 60 * 1000));
    const utcEndTime = new Date(utcTime.getTime() + 60 * 60 * 1000);
    
    startBookingTime = utcTime.toISOString();
    endBookingTime = utcEndTime.toISOString();
    bookDate = utcTime.toISOString();
  } else if (lead.appointment_date && lead.appointment_date !== 'null') {
    // Default times also need adjustment - parse as UTC first
    const defaultStart = new Date(`${lead.appointment_date}T06:30:00Z`);
    const adjustedStart = new Date(defaultStart.getTime() - (ZT_TIMEZONE_OFFSET_MINUTES * 60 * 1000));
    const adjustedEnd = new Date(adjustedStart.getTime() + 60 * 60 * 1000);
    
    startBookingTime = adjustedStart.toISOString();
    endBookingTime = adjustedEnd.toISOString();
    bookDate = adjustedStart.toISOString();
  } else {
    const defaultStart = new Date(`${currentDate}T06:30:00`);
    const adjustedStart = new Date(defaultStart.getTime() - (ZT_TIMEZONE_OFFSET_MINUTES * 60 * 1000));
    const adjustedEnd = new Date(adjustedStart.getTime() + 60 * 60 * 1000);
    
    startBookingTime = adjustedStart.toISOString();
    endBookingTime = adjustedEnd.toISOString();
    bookDate = adjustedStart.toISOString();
  }
  const validName = lead.client_name;
  const validEmail = cleanEmail(lead.client_email);
  
  // Email is required - should not reach here if validation passed
  if (!validEmail) {
    throw new Error('Email is required for ZT booking');
  }
  
  const formattedPhone = formatPhoneNumber(lead.from_number);
  return { startBookingTime, endBookingTime, bookDate, name: validName, email: validEmail, phoneNumber: formattedPhone,
    addressLineOne: addressParts.addressLineOne, addressLineTwo: ' ', city: addressParts.city, state: addressParts.state, country: addressParts.country, zipCode: addressParts.zipCode, companyId: ztCompanyId, description: lead.job_description, source: 'Web' };
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

async function createZTBooking(lead, token, ztCompanyId = 3, timezone = null) {
  const payload = transformLeadToZTBooking(lead, ztCompanyId, timezone);
  const res = await fetch(`${ZT_API_BASE}/api/ob/obr/book/`, {
    method: 'POST',
    headers: {
      'accept': 'application/json, text/plain, */*',
      'content-type': 'application/json',
      'referer': OB_APP_URL,
      'timezone-offset': '-330',
      'user-agent': 'Mozilla/5.0',
      'Authorization': `Bearer ${token}`
    },
    body: JSON.stringify(payload)
  });
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
      // Get ZT token for any user mapped to this agent
      const { token, company_id, timezone } = await getZTTokenForAgent(lead.agent_id);
      const validation = validateLeadData(lead);
      if (!validation.isValid) { await logSyncAttempt(callId, 'failed', null, `Validation failed: ${validation.errors.join(', ')}`); await supabase.from('zt_manual_sync').update({ status:'failed', error_message: validation.errors.join(', '), updated_at: new Date().toISOString() }).eq('call_id', callId); continue; }
      await logSyncAttempt(callId, 'pending');
      const result = await createZTBooking(lead, token, company_id, timezone);
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
      console.error(`Manual error: ${callId}`, e);
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

if (require.main === module) {
  main().catch(err=>{ console.error(err); process.exit(1); });
}
