// ===== SUPABASE INIT =====
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = "https://jziegbpuudswljwasctn.supabase.co";
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp6aWVnYnB1dWRzd2xqd2FzY3RuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODIyMTMwNTQsImV4cCI6MjA5Nzc4OTA1NH0.ywgjRyZv3UxtYgbvVvoLr_Hf02u5wW8yP2v054WD98s";
const sb = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

const COL = {
  master: 'payroll_master',
  harga: 'payroll_harga',
  input: 'payroll_input',
  pinjaman: 'payroll_pinjaman',
  bayar: 'payroll_bayar'
};

// ====================================================================
// SUPABASE HELPER FUNCTIONS
// Wrapper sederhana untuk operasi CRUD ke Supabase Postgres.
//
// Setiap tabel Supabase memiliki kolom:
//   id (text, primary key)
//   data (jsonb)  -> seluruh field record
//   created_at (timestamptz)
// ====================================================================

const db = { __isShimDb: true };
const _SESSION_ID = Math.random().toString(36).slice(2, 8) + Math.random().toString(36).slice(2, 8);

function _genId() {
  // generate random unique id
  return (crypto.randomUUID ? crypto.randomUUID().replace(/-/g,'') : ('id' + Date.now() + Math.random().toString(36).slice(2)));
}

function sbTable(_db, name) {
  return { __table: name };
}

function sbDoc(_db, name, id) {
  // dukung pemanggilan sbDoc(db, name, id) maupun sbDoc(collectionRef, id)
  if (typeof name === 'object' && name && name.__table) {
    return { __table: name.__table, __id: id };
  }
  return { __table: name, __id: id };
}

function sbTimestamp() {
  return new Date().toISOString();
}

function _mkError(rawErr, fallbackCode) {
  const e = new Error((rawErr && rawErr.message) || 'Terjadi kesalahan pada Supabase');
  e.code = (rawErr && (rawErr.code || rawErr.hint)) || fallbackCode || 'supabase/error';
  return e;
}

async function sbInsert(collRef, data) {
  const id = _genId();
  const { error } = await sb.from(collRef.__table).insert({ id, data });
  if (error) throw _mkError(error, 'supabase/insert-failed');
  return { id };
}

async function sbUpsert(docRef, data, options) {
  const merge = options && options.merge;
  if (merge) {
    const { data: existing } = await sb.from(docRef.__table).select('data').eq('id', docRef.__id).maybeSingle();
    const merged = Object.assign({}, existing ? existing.data : {}, data);
    const { error } = await sb.from(docRef.__table).upsert({ id: docRef.__id, data: merged });
    if (error) throw _mkError(error, 'supabase/set-failed');
  } else {
    const { error } = await sb.from(docRef.__table).upsert({ id: docRef.__id, data });
    if (error) throw _mkError(error, 'supabase/set-failed');
  }
}

async function sbUpdate(docRef, partialData) {
  const { data: existing, error: getErr } = await sb.from(docRef.__table).select('data').eq('id', docRef.__id).maybeSingle();
  if (getErr) throw _mkError(getErr, 'supabase/update-failed');
  const merged = Object.assign({}, existing ? existing.data : {}, partialData);
  const { error } = await sb.from(docRef.__table).update({ data: merged }).eq('id', docRef.__id);
  if (error) throw _mkError(error, 'supabase/update-failed');
}

async function sbDelete(docRef) {
  const { error } = await sb.from(docRef.__table).delete().eq('id', docRef.__id);
  if (error) throw _mkError(error, 'supabase/delete-failed');
}

async function sbGetAll(collRef) {
  const { data: rows, error } = await sb.from(collRef.__table).select('id, data');
  if (error) throw _mkError(error, 'supabase/get-failed');
  const docs = (rows || []).map(r => ({
    id: r.id,
    data: () => (r.data || {})
  }));
  return {
    docs,
    forEach: (cb) => docs.forEach(cb)
  };
}

function sbWatch(collRef, onNext, onError, queryModifier) {
  const table = collRef.__table;

  function pushSnapshot(rows) {
    const docs = (rows || []).map(r => ({
      id: r.id,
      data: () => (r.data || {})
    }));
    const snap = {
      docs,
      forEach: (cb) => docs.forEach(cb)
    };
    try { onNext(snap); } catch(e) { console.error(e); }
  }

  // queryModifier (opsional): batasi baris yang diambil dari server
  // (mis. filter tanggal), supaya tabel yang sudah besar tidak perlu
  // di-download utuh tiap kali, lalu disaring lagi di JS.
  function _buildQuery() {
    let q = sb.from(table).select('id, data');
    if (queryModifier) q = queryModifier(q);
    return q;
  }

  // load awal
  _buildQuery().then(({ data: rows, error }) => {
    if (error) { if (onError) onError(_mkError(error, 'supabase/read-failed')); return; }
    pushSnapshot(rows);
  });

  // realtime: setiap ada perubahan di tabel ini, baca ulang (dengan filter yang sama)
  const channel = sb.channel('realtime:' + table + ':' + _SESSION_ID)
    .on('postgres_changes', { event: '*', schema: 'public', table }, () => {
      _buildQuery().then(({ data: rows, error }) => {
        if (error) { if (onError) onError(_mkError(error, 'supabase/read-failed')); return; }
        pushSnapshot(rows);
      });
    })
    .subscribe();

  return () => { sb.removeChannel(channel); };
}

// ===== STATE =====
let MASTER = { pegawai:[], pegawaiData:{}, bagian:[], brand:[], subBagian:{}, jenisPinjaman:[], harga:{}, hargaList:[], brands:[], tambahanItemList:[] };
let _masterDocIds = {}, _hargaDocIds = {};
let _CACHE = { input:[], pinjaman:[], bayar:[] };
let _initFlags = { master:false, harga:false, input:false, pinjaman:false, bayar:false, tambahan:false };
let rekapRows=[], rekapPinRows=[];
let currentTab='pegawai', listType='', dpTarget='';
let _sisaPerNama={}, _nilaiPerNama={};
let _tgl = { rekapMulai:'', rekapAkhir:'', printMulai:'', printAkhir:'' };
let _rcTambahan = {};
let _rcNamaAktif = '';
let _bayarLuarGaji = false;
let _bayarPendapatanBersih = 0;
let _bayarJenisData = [];
let _pegMode = 'select';
let _weekToggleOn = true;
let _qsNeedsRefresh = false;
let _qsLastHash = '';
let _confirmCallback = null;

// ===== HELPERS =====
const $ = id => document.getElementById(id);
const showLoading = t => { $('loadingText').textContent = t||'Memuat...'; $('loadingOverlay').classList.add('active'); };
const hideLoading = () => $('loadingOverlay').classList.remove('active');
const showToast = (msg, dur=3000) => { const t=$('toast'); t.textContent=msg; t.classList.add('show'); setTimeout(()=>t.classList.remove('show'), dur); };
window.bukaPopup = id => {
  const el = $(id); if(!el) return;
  const sheet = el.querySelector('.popup-sheet');
  if(sheet) {
    sheet.style.willChange = 'transform';
    sheet.style.transform = 'translateY(100%)';
  }
  el.style.display = 'flex';
  if(sheet) {
    sheet.offsetHeight; // Force reflow
    requestAnimationFrame(() => {
      sheet.style.transform = 'translateY(0)';
      setTimeout(() => { sheet.style.willChange = 'auto'; }, 300);
    });
  }
};
const bukaPopup = window.bukaPopup;
window.tutupPopup = id => {
  const el = $(id); if(!el) return;
  const sheet = el.querySelector('.popup-sheet');
  if(sheet) {
    sheet.style.willChange = 'transform';
    sheet.style.transform = 'translateY(100%)';
    setTimeout(() => {
      el.style.display = 'none';
      sheet.style.transform = '';
      sheet.style.willChange = 'auto';
    }, 250);
  } else {
    el.style.display = 'none';
  }
};
const fmt = n => Number(n||0).toLocaleString('id-ID');
const fmtTgl = d => `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}/${d.getFullYear()}`;
const setDis = (id, v) => { const el=$(id); if(!el)return; el.disabled=v; };
const resetSelect = (id, items, ph, dis=false) => {
  const el=$(id); if(!el)return;
  el.innerHTML = `<option value="">${ph}</option>`;
  (items||[]).slice().sort((a,b)=>String(a).localeCompare(String(b),'id')).forEach(v => el.innerHTML += `<option value="${v}">${v}</option>`);
  el.value=''; el.disabled=dis; el.style.color='#7B8DB0'; el.style.fontWeight='600';
};
const _getISO = (key) => {
  const v = _tgl[key]||'';
  if(!v) return '';
  // Format DD/MM/YYYY (format internal _tgl)
  if(v.includes('/')) {
    const p = v.split('/');
    if(p.length === 3) return `${p[2]}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`;
  }
  // Format YYYY-MM-DD (fallback jika terisi langsung dari ISO)
  if(/^\d{4}-\d{2}-\d{2}$/.test(v)) return v;
  return '';
};
const _setTgl = (key, label) => {
  _tgl[key] = label;
  const map = { rekapMulai:'tglMulaiTxt', rekapAkhir:'tglAkhirTxt', printMulai:'printTglMulaiTxt', printAkhir:'printTglAkhirTxt' };
  const el=$(map[key]); if(el) el.textContent = label||'--/--/----';
};
const _getSenin = (d) => { const r=new Date(d); const day=r.getDay()||7; r.setDate(r.getDate()-day+1); r.setHours(0,0,0,0); return r; };
const _saveTambahanToSession = () => { try { sessionStorage.setItem('_rcTambahan', JSON.stringify(_rcTambahan)); } catch(e){} };
const _loadTambahanFromSession = () => { try { const raw=sessionStorage.getItem('_rcTambahan'); if(raw) _rcTambahan=JSON.parse(raw); } catch(e){ _rcTambahan={}; } };
function _getWeekId(date) {
  const d = new Date(date); d.setHours(0,0,0,0);
  d.setDate(d.getDate() + 4 - (d.getDay()||7));
  const yearStart = new Date(d.getFullYear(),0,1);
  const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1)/7);
  return d.getFullYear() + '-W' + String(weekNo).padStart(2,'0');
}
function _getMondayISO(date) {
  const d = new Date(date); const day = d.getDay()||7;
  d.setDate(d.getDate() - day + 1); d.setHours(0,0,0,0);
  return d.toISOString().slice(0,10);
}
function _initWeeklyReset() {
  const today = new Date();
  const currentWeekId = _getWeekId(today);
  const savedWeekId = localStorage.getItem('_tambahanWeekId');
  if(savedWeekId !== currentWeekId) {
    _rcTambahan = {};
    sessionStorage.removeItem('_rcTambahan');
    localStorage.setItem('_tambahanWeekId', currentWeekId);
    localStorage.setItem('_tambahanWeekStart', _getMondayISO(today));
    console.log('🔄 Reset tambahan untuk minggu baru:', currentWeekId);
    return true;
  }
  return false;
}
const _tamKey = (t) => (t.bagian||'') + '|' + (t.jenis||'');

// ===== SUPABASE LISTENERS =====
function initMasterListener() {
  sbWatch(sbTable(db, COL.master), snap => {
    const m = { pegawai:[], pegawaiData:{}, bagian:[], brand:[], subBagian:{}, jenisPinjaman:[] };
    _masterDocIds = {};
    snap.forEach(d => {
      const data = d.data();
      _masterDocIds[data.type+'|'+data.nama] = d.id;
      if(data.type==='pegawai') { m.pegawai.push(data.nama); m.pegawaiData[data.nama] = { bagian: data.bagian||'', subbagian: data.subbagian||'' }; }
      if(data.type==='bagian') m.bagian.push(data.nama);
      if(data.type==='brand') m.brand.push(data.nama);
      if(data.type==='jenisPinjaman') m.jenisPinjaman.push(data.nama);
      if(data.type==='subbagian') { if(!m.subBagian[data.bagianInduk]) m.subBagian[data.bagianInduk]=[]; m.subBagian[data.bagianInduk].push(data.nama); }
    });
    MASTER.pegawai = m.pegawai.sort();
    MASTER.pegawaiData = m.pegawaiData;
    MASTER.bagian = m.bagian.sort();
    MASTER.brand = m.brand.sort();
    MASTER.subBagian = m.subBagian;
    MASTER.jenisPinjaman = m.jenisPinjaman.sort();
    MASTER.brands = MASTER.brand;
    _initFlags.master = true;
    _tryInit();
    _autoRefresh();
    if($('listPopup').classList.contains('active')) renderListBody();
  }, err => _showDbError('⚠ Gagal membaca data master.<br>Cek RLS Policy Supabase:<br><b>' + err.code + '</b>'));

  sbWatch(sbTable(db, COL.harga), snap => {
    _hargaDocIds = {};
    MASTER.harga = {};
    MASTER.hargaList = [];
    MASTER.tambahanItemList = [];
    MASTER.tambahan = { uangMakan:0, uangMakanMinggu:0, lembur:0, bonus:0 };
    MASTER.tambahanDocId = '';
    snap.forEach(d => {
      const data = d.data();
      if(data.type === 'tambahan') { MASTER.tambahan = { uangMakan:data.uangMakan||0, uangMakanMinggu:data.uangMakanMinggu||0, lembur:data.lembur||0, bonus:data.bonus||0 }; MASTER.tambahanDocId = d.id; return; }
      if(data.type === 'tambahan_item') {
        const key = 'tambahan_item|' + (data.bagian||'') + '|' + (data.jenis||'');
        if(!MASTER.tambahanItemList) MASTER.tambahanItemList = [];
        if(_hargaDocIds[key]) { const oldDocId = _hargaDocIds[key]; sbDelete(sbDoc(db, COL.harga, oldDocId)).catch(()=>{}); const existIdx = MASTER.tambahanItemList.findIndex(t => t.id === oldDocId); if(existIdx >= 0) MASTER.tambahanItemList.splice(existIdx, 1); }
        _hargaDocIds[key] = d.id;
        MASTER.tambahanItemList.push({ id:d.id, bagian:data.bagian||'', jenis:data.jenis||'', harga:data.harga||0, satuan:data.satuan||'HARI' });
        return;
      }
      if(data.brand) {
        const key = data.pekerjaan ? (data.brand + '|' + data.pekerjaan) : data.brand;
        MASTER.harga[key] = data.harga;
        _hargaDocIds[key] = d.id;
        MASTER.hargaList.push({ id:d.id, brand:data.brand, bagian:data.bagian||'', pekerjaan:data.pekerjaan||'', harga:data.harga });
      }
    });
    _initFlags.harga = true;
    _tryInit();
    if($('hargaPage').classList.contains('active')) { _populateTamPekerjaanSelect(); _populateHrgBrandSelect(); }
  }, err => _showDbError('⚠ Gagal membaca data harga.<br>Cek RLS Policy Supabase:<br><b>' + err.code + '</b>'));
}

function initDataListeners() {
  // OPTIMASI: Hanya ambil data 30 hari terakhir saat load awal
  const _30hariLalu = new Date();
  _30hariLalu.setDate(_30hariLalu.getDate() - 30);
  const _cutoffISO = _30hariLalu.toISOString().slice(0,10);
  const dateFilter = q => q.gte('data->>tanggalISO', _cutoffISO);

  sbWatch(sbTable(db, COL.input), snap => { _CACHE.input = snap.docs.map(d => ({ id:d.id, ...d.data() })); _initFlags.input = true; _tryInit(); _autoRefresh(); }, err => _showDbError('⚠ Gagal membaca payroll_input.<br>Cek RLS Policy Supabase:<br><b>' + err.code + '</b>'), dateFilter);
  sbWatch(sbTable(db, COL.pinjaman), snap => { _CACHE.pinjaman = snap.docs.map(d => ({ id:d.id, ...d.data() })); _initFlags.pinjaman = true; _tryInit(); _autoRefresh(); }, err => _showDbError('⚠ Gagal membaca payroll_pinjaman.<br>Cek RLS Policy Supabase:<br><b>' + err.code + '</b>'), dateFilter);
  sbWatch(sbTable(db, COL.bayar), snap => { _CACHE.bayar = snap.docs.map(d => ({ id:d.id, ...d.data() })); _initFlags.bayar = true; _tryInit(); _autoRefresh(); }, err => _showDbError('⚠ Gagal membaca payroll_bayar.<br>Cek RLS Policy Supabase:<br><b>' + err.code + '</b>'), dateFilter);
  // Hanya butuh data ~2 minggu terakhir untuk rekap, jadi filter di server
  // (sebelumnya download SELURUH histori 'tambahan' lalu baru disaring di JS,
  // ini yang paling bikin loading lama kalau datanya sudah banyak)
  const _today00 = new Date();
  const _day00 = _today00.getDay() || 7;
  const _senin00 = new Date(_today00); _senin00.setDate(_today00.getDate() - _day00 + 1); _senin00.setHours(0,0,0,0);
  const _duaMingguLalu00 = new Date(_senin00); _duaMingguLalu00.setDate(_senin00.getDate() - 7);
  const _cutoffTambahanISO = _duaMingguLalu00.toISOString().slice(0,10);

  sbWatch(sbTable(db, 'tambahan'), snap => {
    _rcTambahan = {};
    const _today = new Date();
    const _day = _today.getDay() || 7;
    const _senin = new Date(_today); _senin.setDate(_today.getDate() - _day + 1); _senin.setHours(0,0,0,0);
    const _minggu = new Date(_senin); _minggu.setDate(_senin.getDate() + 6); _minggu.setHours(23,59,59,999);
    const _seninISO = _senin.toISOString().slice(0,10);
    const _mingguISO = _minggu.toISOString().slice(0,10);
    snap.forEach(docSnap => {
      const data = docSnap.data();
      const nama = data.nama;
      if(!nama || nama.trim()==='' || nama==='-') return;
      const tglISO = (data.tanggalISO || '').slice(0,10);
      if(tglISO < _seninISO || tglISO > _mingguISO) return;
      if(data.itemsJSON) {
        try {
          const itemsRaw = JSON.parse(data.itemsJSON);
          const items = Object.fromEntries(
            Object.entries(itemsRaw).filter(([,it]) => (it.qty||0) > 0)
          );
          // FIX 9: Simpan dengan weekId untuk tracking
          const weekId = data.weekId || _getWeekId(new Date(tglISO));
          if(!_rcTambahan[nama] || tglISO >= (_rcTambahan[nama]._tanggalISO||'')) {
            const calcTotal = Object.values(items).reduce((s, it) => s + (it.jumlah || 0), 0);
            _rcTambahan[nama] = {
              items,
              total: data.total > 0 ? data.total : calcTotal,
              _tanggalISO: tglISO,
              _weekId: weekId
            };
          }
          return;
        } catch(e) {}
      }
      if(!_rcTambahan[nama] || tglISO >= (_rcTambahan[nama]._tanggalISO||'')) {
        const _legacyItems = {};
        const _mTam = MASTER.tambahan || {};
        if((data.hariMakan||0)>0||data.jumlahMakan>0) { const _hMakan = data.jumlahMakan&&data.hariMakan ? Math.round(data.jumlahMakan/data.hariMakan) : (_mTam.uangMakan||0); const _qMakan = data.hariMakan||0; const _jMakan = data.jumlahMakan||0; _legacyItems['|UANG MAKAN'] = { jenis:'UANG MAKAN', harga: _hMakan, qty: _qMakan, jumlah: _jMakan > 0 ? _jMakan : _qMakan * _hMakan }; }
        if((data.hariMakanMinggu||0)>0||data.jumlahMakanMinggu>0) { const _hMakanM = data.jumlahMakanMinggu&&data.hariMakanMinggu ? Math.round(data.jumlahMakanMinggu/data.hariMakanMinggu) : (_mTam.uangMakanMinggu||0); const _qMakanM = data.hariMakanMinggu||0; const _jMakanM = data.jumlahMakanMinggu||0; _legacyItems['|UANG MAKAN MINGGU'] = { jenis:'UANG MAKAN MINGGU', harga: _hMakanM, qty: _qMakanM, jumlah: _jMakanM > 0 ? _jMakanM : _qMakanM * _hMakanM }; }
        if((data.hariLembur||0)>0||data.jumlahLembur>0) { const _hLembur = data.jumlahLembur&&data.hariLembur ? Math.round(data.jumlahLembur/data.hariLembur) : (_mTam.lembur||0); const _qLembur = data.hariLembur||0; const _jLembur = data.jumlahLembur||0; _legacyItems['|LEMBUR'] = { jenis:'LEMBUR', harga: _hLembur, qty: _qLembur, jumlah: _jLembur > 0 ? _jLembur : _qLembur * _hLembur }; }
        if((data.hariBonus||0)>0||data.jumlahBonus>0) { const _hBonus = data.jumlahBonus&&data.hariBonus ? Math.round(data.jumlahBonus/data.hariBonus) : (_mTam.bonus||0); const _qBonus = data.hariBonus||0; const _jBonus = data.jumlahBonus||0; _legacyItems['|BONUS'] = { jenis:'BONUS', harga: _hBonus, qty: _qBonus, jumlah: _jBonus > 0 ? _jBonus : _qBonus * _hBonus }; }
        const _legacyCalcTotal = Object.values(_legacyItems).reduce((s,it)=>s+(it.jumlah||0),0);
        _rcTambahan[nama] = { makan: { hari: data.hariMakan||0, jumlah: data.jumlahMakan||0 }, makanMinggu: { hari: data.hariMakanMinggu||0, jumlah: data.jumlahMakanMinggu||0 }, lembur: { hari: data.hariLembur||0, jumlah: data.jumlahLembur||0 }, bonus: { hari: data.hariBonus||0, jumlah: data.jumlahBonus||0 }, items: _legacyItems, total: data.total > 0 ? data.total : _legacyCalcTotal, _tanggalISO: tglISO };
      }
    });
    _saveTambahanToSession();
    _initFlags.tambahan = true;
    _tryInit();
    _autoRefresh();
  }, err => { console.warn('⚠️ Gagal listener tambahan:', err.message); _loadTambahanFromSession(); _initFlags.tambahan = true; _tryInit(); }, q => q.gte('data->>tanggalISO', _cutoffTambahanISO));
}

let _initTimeout;
function _tryInit() {
  if(Object.values(_initFlags).every(Boolean)) { clearTimeout(_initTimeout); hideLoading(); loadDashboardDana(); }
}
function _showDbError(msg) {
  clearTimeout(_initTimeout);
  const el = $('loadingText');
  el.innerHTML = msg;
  el.style.color = '#EF4444';
  el.style.textAlign = 'center';
  el.style.fontSize = '11px';
  el.style.letterSpacing = '0';
  el.style.textTransform = 'none';
  el.style.maxWidth = '260px';
  el.style.lineHeight = '1.5';
  const retryBtn = $('loadingRetryBtn');
  if(retryBtn) retryBtn.style.display = 'block';
}

let _refreshDebounceTimer = null;
let _dashboardStale = false;
function _autoRefresh() {
  clearTimeout(_refreshDebounceTimer);
  _refreshDebounceTimer = setTimeout(() => {
    const onHome    = !$('inputPage').classList.contains('active') &&
                      !$('rekapPage').classList.contains('active') &&
                      !$('settingPage').classList.contains('active') &&
                      !$('hargaPage').classList.contains('active');
    const onRekap   = $('rekapPage').classList.contains('active');
    const onSetting = $('settingPage').classList.contains('active');
    const onInput   = $('inputPage').classList.contains('active');

    if(onHome)  { loadDashboardDana(); }
    if(onRekap) { muatRekapPegawai(); muatRekapPinjaman(); }
    if(onSetting && _pegMode === 'select') {
      const namaTerpilih = $('set_peg_dropdown').value;
      const dd = $('set_peg_dropdown');
      dd.innerHTML = '<option value="">dropdown...</option>';
      MASTER.pegawai.slice().sort((a,b)=>String(a).localeCompare(String(b),'id')).forEach(p => dd.innerHTML += `<option value="${p}">${p}</option>`);
      if(namaTerpilih && MASTER.pegawai.includes(namaTerpilih)) { dd.value = namaTerpilih; } else { dd.value = ''; $('set_peg_bagian_ro').textContent = '—'; $('pegRowHapusSave').style.display = 'none'; }
    }
    if(onInput) { refreshQuickStats(); }

    if(!onHome) { _dashboardStale = true; }
  }, 400);
}

function getInputDataLocal(dari, sampai) {
  return _CACHE.input.filter(r => { if(dari && r.tanggalISO < dari) return false; if(sampai && r.tanggalISO > sampai) return false; return true; });
}

function getRekapPinjamanPerJenisLocal(nama, dari='', sampai='') {
  const docPerJenis = {};
  _CACHE.pinjaman.filter(p => p.nama === nama).forEach(p => {
    const j = p.jenis || '(lainnya)';
    if (!docPerJenis[j]) docPerJenis[j] = [];
    docPerJenis[j].push({ id: p.id, nominal: p.nominal || 0, tanggalISO: p.tanggalISO || '' });
  });
  Object.keys(docPerJenis).forEach(j => { docPerJenis[j].sort((a,b) => a.tanggalISO.localeCompare(b.tanggalISO)); });
  const bayarPerJenis = {};
  _CACHE.bayar.filter(b => b.nama === nama).forEach(b => { const j = b.jenisPinjaman || '(lainnya)'; bayarPerJenis[j] = (bayarPerJenis[j] || 0) + (b.bayar || 0); });
  const result = [];
  Object.keys(docPerJenis).forEach(j => {
    let sisaBayar = bayarPerJenis[j] || 0;
    docPerJenis[j].forEach(doc => {
      const terbayar = Math.min(sisaBayar, doc.nominal);
      sisaBayar -= terbayar;
      const sisa = doc.nominal - terbayar;
      if (sisa > 0) { result.push({ jenis: j, pinjaman: doc.nominal, bayar: terbayar, sisa }); }
    });
  });
  return result;
}

function getRekapPinjamanLocal() {
  const docMap = {};
  _CACHE.pinjaman.forEach(v => { const j = v.jenis || '(lainnya)'; const key = v.nama + '||' + j; if (!docMap[key]) docMap[key] = []; docMap[key].push({ id: v.id, nominal: v.nominal || 0, tanggalISO: v.tanggalISO || '' }); });
  Object.keys(docMap).forEach(key => { docMap[key].sort((a,b) => a.tanggalISO.localeCompare(b.tanggalISO)); });
  const bayarMap = {};
  _CACHE.bayar.forEach(v => { const j = v.jenisPinjaman || '(lainnya)'; const key = v.nama + '||' + j; bayarMap[key] = (bayarMap[key] || 0) + (v.bayar || 0); });
  const allNama = new Set(_CACHE.pinjaman.map(v => v.nama));
  return [...allNama].map(nama => {
    const semuaJenis = new Set(_CACHE.pinjaman.filter(v => v.nama === nama).map(v => v.jenis || '(lainnya)'));
    let totalPinjamanAktif = 0, totalBayarAktif = 0;
    const jenisAktif = new Set();
    semuaJenis.forEach(j => {
      const key = nama + '||' + j;
      const docs = docMap[key] || [];
      let sisaBayar = bayarMap[key] || 0;
      docs.forEach(doc => {
        const terbayar = Math.min(sisaBayar, doc.nominal);
        sisaBayar -= terbayar;
        const sisa = doc.nominal - terbayar;
        if (sisa > 0) { totalPinjamanAktif += doc.nominal; totalBayarAktif += terbayar; if (j !== '(lainnya)') jenisAktif.add(j); }
      });
    });
    return { nama, jenis: jenisAktif.size > 0 ? [...jenisAktif].join(', ') : '-', pinjaman: totalPinjamanAktif, bayar: totalBayarAktif, sisa: Math.max(0, totalPinjamanAktif - totalBayarAktif) };
  });
}

function getRincianLocal(nama, dari, sampai) {
  const items = getInputDataLocal(dari, sampai).filter(r => r.nama===nama);
  let subtotal = 0;
  const rows = items.map(r => { subtotal += (r.jumlah||0); return { id:r.id, jenis:r.jenis, qty:r.qty, harga:r.harga, jumlah:r.jumlah, tanggalISO:r.tanggalISO||'' }; });
  const bayarItems = _CACHE.bayar.filter(b => { if(b.nama!==nama) return false; if(dari && b.tanggalISO < dari) return false; if(sampai && b.tanggalISO > sampai) return false; if(b.luarGaji) return false; return true; });
  const potongan = bayarItems.reduce((s,b) => s+(b.bayar||0), 0);
  const bagian = items.length>0 ? (items[0].bagian||'-') : '-';
  // FIX 7: Include tambahan dalam return
  const tam = _rcTambahan[nama];
  let tamTotal = 0;
  if(tam) {
    const tamTgl = tam._tanggalISO || '';
    const inRange = (!dari || tamTgl >= dari) && (!sampai || tamTgl <= sampai);
    if(inRange) {
      tamTotal = tam.total > 0 ? tam.total :
        (tam.items ? Object.values(tam.items).reduce((s,it)=>s+(it.jumlah||0),0) : 0);
    }
  }
  return { items:rows, subtotal, potongan, tamTotal, total:subtotal+tamTotal-potongan, bagian };
}

// ===== WINDOW FUNCTIONS =====
// Flag: user keluar dari inputPage tanpa save
let _inputExitedWithoutSave = false;

function _doResetInputForm() {
  resetSelect('inp_nama', MASTER.pegawai, 'Nama Pegawai....');
  const bagianEl = $('inp_bagian');
  bagianEl.value = '';
  bagianEl.style.color = '#7B8DB0';
  resetSelect('inp_jenis', [], '-- Pilih Pekerjaan --', true);
  resetSelect('inp_brand', [], '-- Pilih Brand --', true);
  $('inp_qty').value = ''; $('inp_qty_display').value = '';
  $('inp_qty_trigger').style.opacity = '0.4'; $('inp_qty_trigger').style.pointerEvents = 'none';
  setDis('btnTambahInput', true); _styleBtnTambah(false);
  _stagingList = [];
  _renderStaging();
}

// ===== TAB INPUT / RIWAYAT (halaman Input Data) =====
window.gantiTabInput = (tab) => {
  const panelInput = $('inpPanelInput');
  const panelRiwayat = $('inpPanelRiwayat');
  const btnInput = $('inpTabBtnInput');
  const btnRiwayat = $('inpTabBtnRiwayat');
  if (!panelInput || !panelRiwayat || !btnInput || !btnRiwayat) return;
  if (tab === 'riwayat') {
    panelInput.style.display = 'none';
    panelRiwayat.style.display = 'block';
    btnRiwayat.classList.add('active');
    btnInput.classList.remove('active');
    const pageBodyEl = panelRiwayat.closest('.page-body');
    if (pageBodyEl) {
      pageBodyEl.scrollTop = 0;
      // Kunci layar Riwayat agar tidak bisa digeser atas/bawah,
      // karena kartu STATISTIK sudah didesain pas 1 layar penuh.
      pageBodyEl.style.overflowY = 'hidden';
      pageBodyEl.style.touchAction = 'none';
    }
    refreshQuickStats();
    requestAnimationFrame(() => requestAnimationFrame(_sizeQsCard));
  } else {
    panelInput.style.display = 'block';
    panelRiwayat.style.display = 'none';
    btnInput.classList.add('active');
    btnRiwayat.classList.remove('active');
    // Tab Input tetap perlu bisa di-scroll (form-nya panjang)
    const pageBodyEl = panelInput.closest('.page-body');
    if (pageBodyEl) {
      pageBodyEl.style.overflowY = 'auto';
      pageBodyEl.style.touchAction = '';
    }
  }
};

window.bukaInputData = () => {
  // Selalu mulai dari tab Input saat halaman dibuka
  gantiTabInput('input');
  // Jika sebelumnya keluar tanpa save, reset semua
  if (_inputExitedWithoutSave) {
    _inputExitedWithoutSave = false;
    _doResetInputForm();
  } else {
    // Jangan reset staging list saat buka ulang (biarkan jika ada item)
    resetSelect('inp_nama', MASTER.pegawai, 'Nama Pegawai....');
    const bagianEl = $('inp_bagian');
    bagianEl.value = '';
    bagianEl.style.color = '#7B8DB0';
    resetSelect('inp_jenis', [], '-- Pilih Pekerjaan --', true);
    resetSelect('inp_brand', [], '-- Pilih Brand --', true);
    $('inp_qty').value = ''; $('inp_qty_display').value = '';
    $('inp_qty_trigger').style.opacity = '0.4'; $('inp_qty_trigger').style.pointerEvents = 'none';
    setDis('btnTambahInput', true); _styleBtnTambah(false);
    _renderStaging();
  }
  $('inputPage').classList.add('active');
  history.pushState({ page: 'inputPage' }, '');
  refreshQuickStats();
};

function _tutupInputDataLangsung() {
  _inputExitedWithoutSave = true;
  $('inputPage').classList.remove('active');
  if (history.state && history.state.page === 'inputPage') history.back();
  if(_dashboardStale) { _dashboardStale = false; loadDashboardDana(); }
}

window.tutupInputData = () => {
  if (_stagingList.length > 0) {
    bukaConfirm({
      icon: '<i class="fa-solid fa-triangle-exclamation"></i>',
      iconBg: '#FEF3C7',
      iconColor: '#D97706',
      title: 'Data Belum Disimpan',
      msg: `Ada ${_stagingList.length} item yang belum disimpan. Yakin mau keluar? Data akan hilang.`,
      okLabel: 'Ya, Keluar',
      okBg: 'linear-gradient(135deg, #EF4444, #F87171)',
      callback: _tutupInputDataLangsung
    });
  } else {
    $('inputPage').classList.remove('active');
    if (history.state && history.state.page === 'inputPage') history.back();
  }
};

window.onInpNama = () => {
  const nama = $('inp_nama').value; $('inp_nama').style.color = nama ? '#1A2340' : '#7B8DB0';
  const peg = MASTER.pegawaiData[nama] || { bagian:'', subbagian:'' };
  const bagianEl = $('inp_bagian');
  bagianEl.value = peg.bagian;
  bagianEl.style.color = peg.bagian ? '#1A2340' : '#7B8DB0';
  const brandsForBagian = [...new Set(MASTER.hargaList.filter(h => (!peg.bagian || !h.bagian || h.bagian === peg.bagian)).map(h => h.brand))].filter(Boolean).sort();
  resetSelect('inp_brand', brandsForBagian, '-- Pilih Brand --', !nama || !brandsForBagian.length);
  resetSelect('inp_jenis', [], '-- Pilih Pekerjaan --', true);
  $('inp_qty').value = ''; $('inp_qty_display').value = ''; $('inp_qty_trigger').style.opacity = '0.4'; $('inp_qty_trigger').style.pointerEvents = 'none';
  setDis('btnTambahInput', true); _styleBtnTambah && _styleBtnTambah(false);
};

window.onInpBrand = () => {
  const brand = $('inp_brand').value; $('inp_brand').style.color = brand ? '#1A2340' : '#7B8DB0';
  const bagian = $('inp_bagian').value;
  const pekerjaanList = [...new Set(MASTER.hargaList.filter(h => h.brand === brand && (!bagian || !h.bagian || h.bagian === bagian) && h.pekerjaan).map(h => h.pekerjaan))].sort();
  resetSelect('inp_jenis', pekerjaanList, '-- Pilih Pekerjaan --', !brand || !pekerjaanList.length);
  $('inp_qty').value = ''; $('inp_qty_display').value = ''; $('inp_qty_trigger').style.opacity = '0.4'; $('inp_qty_trigger').style.pointerEvents = 'none';
  setDis('btnTambahInput', true); _styleBtnTambah && _styleBtnTambah(false);
};

window.onInpJenis = () => {
  const brand = $('inp_brand').value;
  const jenis = $('inp_jenis').value; $('inp_jenis').style.color = jenis ? '#1A2340' : '#7B8DB0';
  $('inp_qty').value = ''; $('inp_qty_display').value = '';
  $('inp_qty_trigger').style.opacity = '0.4'; $('inp_qty_trigger').style.pointerEvents = 'none';
  setDis('btnTambahInput', true); 
  _styleBtnTambah && _styleBtnTambah(false);
  if(brand && jenis) {
    const key = brand + '|' + jenis;
    const hargaVal = MASTER.harga[key] || 0;
    if(hargaVal) { $('inp_qty_trigger').style.opacity = '1'; $('inp_qty_trigger').style.pointerEvents = 'auto'; }
  }
};

window.onInpQty = () => {
  const brand = $('inp_brand').value;
  const jenis = $('inp_jenis').value;
  const key = brand + '|' + jenis;
  const harga = MASTER.harga[key] || 0;
  const qty = parseInt($('inp_qty').value) || 0;
  $('inp_qty_display').style.color = qty > 0 ? '#1A2340' : '#7B8DB0';
  if(qty > 0 && harga > 0) { setDis('btnTambahInput', false); _styleBtnTambah(true); }
  else { setDis('btnTambahInput', true); _styleBtnTambah(false); }
};

// Styling helper untuk tombol tambah
function _styleBtnTambah(aktif) {
  const btn = $('btnTambahInput');
  if(!btn) return;
  if(aktif) {
    btn.style.background = 'linear-gradient(135deg, #059669, #10B981)'; // Green
    btn.style.color = '#fff';
    btn.style.boxShadow = '0 4px 16px rgba(16, 185, 129, 0.28)';
    btn.style.cursor = 'pointer';
  } else {
    btn.style.background = '#E8EEF5';
    btn.style.color = '#7B8DB0';
    btn.style.boxShadow = 'none';
    btn.style.cursor = 'not-allowed';
  }
}

// ===== STAGING LIST =====
let _stagingList = [];

function _renderStaging() {
  const tbody = $('stagingTbody');
  const section = $('stagingSection');
  const countEl = $('stagingCount');
  const totalEl = $('stagingTotal');
  if(!tbody) return;
  // Section selalu terlihat, tidak di-hide
  if(_stagingList.length === 0) {
    tbody.innerHTML = `<tr><td colspan="6">
      <div class="empty-state-box" style="padding:24px 20px;">
        <div class="empty-state-box-text">Belum ada item yang ditambahkan</div>
      </div>
    </td></tr>`;
    countEl.textContent = '0';
    totalEl.textContent = 'Rp 0';
    setDis('btnSimpanInput', true);
    const btnSimpan = $('btnSimpanInput');
    if(btnSimpan) {
      btnSimpan.style.background = 'var(--muted)'; // Use muted color for disabled
      btnSimpan.style.color = '#fff';
      btnSimpan.style.cursor = 'not-allowed';
      btnSimpan.style.boxShadow = 'none';
    }
    return;
  }
  countEl.textContent = _stagingList.length;
  const grandTotal = _stagingList.reduce((s, i) => s + i.jumlah, 0);
  totalEl.textContent = 'Rp ' + fmt(grandTotal);
  tbody.innerHTML = _stagingList.map((item, idx) => `
    <tr style="border-top:1px solid var(--muted);${idx % 2 === 1 ? 'background:var(--bg2);' : ''}">
      <td style="padding:9px 10px;font-size:12px;font-weight:700;color:var(--text2);white-space:nowrap;max-width:80px;overflow:hidden;text-overflow:ellipsis;">${item.brand}</td>
      <td style="padding:9px 6px;font-size:11px;color:var(--text2);max-width:90px;overflow:hidden;text-overflow:ellipsis;">${item.jenis}</td>
      <td style="padding:9px 6px;font-size:13px;font-weight:800;color:var(--primary3);text-align:center;">${item.qty}</td>
      <td style="padding:9px 6px;font-size:11px;color:var(--muted);text-align:right;white-space:nowrap;">Rp ${fmt(item.harga)}</td>
      <td style="padding:9px 6px;font-size:12px;font-weight:800;color:var(--green);text-align:right;white-space:nowrap;">Rp ${fmt(item.jumlah)}</td>
      <td style="padding:9px 8px 9px 4px;text-align:center;">
        <button onclick="hapusStaging(${idx})" title="Hapus item" style="background:none;border:none;color:#EF4444;font-size:13px;cursor:pointer;padding:2px 6px;border-radius:6px;">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </td>
    </tr>
  `).join('');
  // Enable simpan button
  const btnSimpan = $('btnSimpanInput');
  if(btnSimpan) {
    btnSimpan.disabled = false;
    btnSimpan.style.background = 'linear-gradient(135deg, var(--primary3), var(--primary))'; // Blue
    btnSimpan.style.color = '#fff';
    btnSimpan.style.cursor = 'pointer';
    btnSimpan.style.boxShadow = '0 4px 16px rgba(26, 86, 219, 0.28)';
  }
}

window.hapusStaging = (idx) => {
  _stagingList.splice(idx, 1);
  _renderStaging();
};

window.tambahKeStaging = () => {
  const nama = $('inp_nama').value;
  const bagian = $('inp_bagian').value;
  const jenis = $('inp_jenis').value;
  const brand = $('inp_brand').value;
  const qty = parseInt($('inp_qty').value) || 0;
  if(!nama || !brand || !jenis || qty < 1) { showToast('⚠ Lengkapi semua field!'); return; }
  const key = brand + '|' + jenis;
  const harga = MASTER.harga[key] || MASTER.harga[brand] || 0;
  if(!harga) { showToast('⚠ Harga belum diset!'); return; }
  _stagingList.push({ nama, bagian, jenis, brand, qty, harga, jumlah: harga * qty });
  _renderStaging();
  // Reset brand, pekerjaan, qty — tapi nama & bagian tetap
  resetSelect('inp_brand', [...new Set(MASTER.hargaList.filter(h => (!bagian || !h.bagian || h.bagian === bagian) && h.brand).map(h => h.brand))].sort(), '-- Pilih Brand --', false);
  resetSelect('inp_jenis', [], '-- Pilih Pekerjaan --', true);
  $('inp_qty').value = '';
  $('inp_qty').value = ''; $('inp_qty_display').value = ''; $('inp_qty_trigger').style.opacity = '0.4'; $('inp_qty_trigger').style.pointerEvents = 'none';
  setDis('btnTambahInput', true);
  _styleBtnTambah(false);
  showToast('✅ Item ditambahkan!');
};

window.simpanInputData = async () => {
  if(_stagingList.length === 0) { showToast('⚠ Tidak ada item untuk disimpan!'); return; }
  const btn = $('btnSimpanInput');
  btn.textContent = 'Menyimpan...'; btn.disabled = true;
  showLoading('Menyimpan data...');
  try {
    const tanggal = fmtTgl(new Date());
    const tanggalISO = new Date().toISOString().slice(0,10);
    const dataToInsert = _stagingList.map(item => ({
      id: _genId(),
      data: { tanggal, tanggalISO, nama:item.nama, bagian:item.bagian, jenis:item.jenis, brand:item.brand, qty:item.qty, harga:item.harga, jumlah:item.jumlah, waktu:sbTimestamp() }
    }));
    const { error } = await sb.from(COL.input).insert(dataToInsert);
    if (error) throw error;
    const n = _stagingList.length;
    _stagingList = [];
    _inputExitedWithoutSave = false;
    _renderStaging();
    showToast('✅ ' + n + ' data tersimpan!');
    btn.textContent = 'SIMPAN';
    // Reset semua
    resetSelect('inp_nama', MASTER.pegawai, 'Nama Pegawai....');
    const bagianEl = $('inp_bagian'); bagianEl.value = ''; bagianEl.style.color = '#7B8DB0';
    resetSelect('inp_jenis', [], '-- Pilih Pekerjaan --', true);
    resetSelect('inp_brand', [], '-- Pilih Brand --', true);
    $('inp_qty').value = ''; $('inp_qty').value = ''; $('inp_qty_display').value = ''; $('inp_qty_trigger').style.opacity = '0.4'; $('inp_qty_trigger').style.pointerEvents = 'none';
    setDis('btnTambahInput', true); _styleBtnTambah(false);
    _qsNeedsRefresh = true;
  } catch(e) { showToast('❌ Gagal: '+e.message); btn.textContent = 'SIMPAN'; btn.disabled = false; }
  finally { hideLoading(); }
};

// ===== STATE NUMPAD =====
let _numpadTarget = null; // 'pinjaman' | 'bayar'
let _numpadValue = '';
let _numpadMaxValue = null;
let _numpadCallback = null;
let _numpadBayarIndex = null;

// ===== STATE DRAFT PINJAMAN (multi-entry sebelum simpan) =====
let _pinDraftList = []; // [{nama, jenis, nominal}]

// Isi dropdown Jenis Pinjaman untuk pegawai tertentu, sambil
// menyembunyikan jenis yang sudah ada di draft list untuk pegawai itu
// (mencegah pilih jenis yang sama dua kali).
function populatePinJenisForNama(nama) {
  const dipakai = _pinDraftList.filter(p => p.nama === nama).map(p => p.jenis);
  const tersedia = MASTER.jenisPinjaman.filter(j => !dipakai.includes(j));
  resetSelect('pin_jenis', tersedia, '-- Pilih Jenis --');
}

// ===== PINJAMAN FUNCTIONS =====

window.bukaPinjaman = () => {
  resetSelect('pin_nama', MASTER.pegawai, '-- Pilih Pegawai --');
  resetSelect('pin_jenis', MASTER.jenisPinjaman, '-- Pilih Jenis --');
  
  $('pin_nama').disabled = false;
  $('pinNamaGroup').classList.remove('popup-field-locked');
  $('btnGantiPegawaiPin').classList.add('hidden');
  $('pinNamaLockedHint').classList.add('hidden');

  $('pin_jenis').disabled = true;
  $('pin_nominal_display').value = '';
  $('pin_nominal').value = '';
  $('pinNominalHint').textContent = 'Ketuk field di atas untuk membuka numpad';
  $('pinNominalHint').style.color = '#7B8DB0';
  const _bp=$('btnSimpanPin');if(_bp)_bp.innerHTML='<i class="fa-solid fa-floppy-disk"></i><span>Simpan Pinjaman</span>';
  setDis('btnSimpanPin', true);
  
  $('pinJenisGroup').className = 'popup-form-group hidden';
  $('pinNominalGroup').className = 'popup-form-group hidden';
  $('pinAddMoreRow').className = 'popup-add-more-row hidden';

  _pinDraftList = [];
  renderPinDraftList();
  
  const popup = $('pinjamanPopup');
  const sheet = popup.querySelector('.popup-sheet');
  if (sheet && popup.classList.contains('active')) {
    sheet.classList.add('entering');
    sheet.style.willChange = 'transform';
  }
  popup.classList.add('active');
  if (sheet) {
    sheet.offsetHeight;
    requestAnimationFrame(() => {
      sheet.classList.remove('entering'); // This will trigger the slide-up animation
      setTimeout(() => { sheet.style.willChange = 'auto'; }, 300); // Clean up after animation
    });
  }
};

window.onPinNama = () => {
  const nama = $('pin_nama').value;
  $('pin_nama').style.color = nama ? '#1A2340' : '#7B8DB0';
  
  if (!nama) {
    resetSelect('pin_jenis', MASTER.jenisPinjaman, '-- Pilih Jenis --');
    $('pin_jenis').disabled = true;
    $('pin_nominal_display').value = '';
    $('pin_nominal').value = '';
    $('pin_nominal_display').disabled = true;
    setDis('btnSimpanPin', _pinDraftList.length === 0);
    
    $('pinJenisGroup').className = 'popup-form-group hidden';
    $('pinNominalGroup').className = 'popup-form-group hidden';
    $('pinAddMoreRow').className = 'popup-add-more-row hidden';
    return;
  }
  
  $('pin_jenis').disabled = false;
  populatePinJenisForNama(nama);
  $('pinJenisGroup').className = 'popup-form-group visible';
  $('pin_jenis').focus();
};

window.onPinJenis = () => {
  const jenis = $('pin_jenis').value;
  $('pin_jenis').style.color = jenis ? '#1A2340' : '#7B8DB0';
  
  if (!jenis) {
    $('pin_nominal_display').value = '';
    $('pin_nominal').value = '';
    setDis('btnSimpanPin', _pinDraftList.length === 0);
    $('pinNominalGroup').className = 'popup-form-group hidden';
    $('pinAddMoreRow').className = 'popup-add-more-row hidden';
    return;
  }
  
  $('pinNominalGroup').className = 'popup-form-group visible';
};

window.cekSimpanPin = () => {
  const nominal = parseInt($('pin_nominal').value) || 0;
  const ok = !!$('pin_jenis').value && nominal > 0;
  setDis('btnSimpanPin', !ok && _pinDraftList.length === 0);
  
  const hint = $('pinNominalHint');
  if (nominal > 0) {
    hint.innerHTML = '<span style="color:var(--primary3);font-weight:800;">Rp ' + fmt(nominal) + '</span>';
  } else {
    hint.textContent = 'Ketuk field di atas untuk membuka numpad';
    hint.style.color = '#7B8DB0';
  }

  $('pinAddMoreRow').className = 'popup-add-more-row' + (ok ? '' : ' hidden');
  updateSimpanPinLabel();
};

// Buka kembali kunci Nama Pegawai. Karena draft list yang sudah ada
// melekat pada nama sebelumnya, admin harus konfirmasi karena daftar
// tersebut akan dikosongkan saat ganti pegawai.
window.gantiPegawaiPinjaman = () => {
  bukaConfirm({
    icon: '<i class="fa-solid fa-rotate-left"></i>',
    iconBg: 'rgba(75, 142, 248, 0.12)',
    iconColor: '#1A56DB',
    title: 'Ganti Pegawai?',
    msg: `Daftar pinjaman yang sudah ditambahkan (<b>${_pinDraftList.length} entri</b>) untuk pegawai ini akan dihapus. Lanjutkan ganti pegawai?`,
    okLabel: 'Ya, Ganti Pegawai',
    okBg: 'linear-gradient(135deg, #1A56DB, #4B8EF8)',
    callback: () => {
      _pinDraftList = [];
      renderPinDraftList();

      resetSelect('pin_nama', MASTER.pegawai, '-- Pilih Pegawai --');
      $('pin_nama').disabled = false;
      $('pinNamaGroup').classList.remove('popup-field-locked');
      $('btnGantiPegawaiPin').classList.add('hidden');
      $('pinNamaLockedHint').classList.add('hidden');

      $('pin_jenis').disabled = true;
      $('pin_nominal_display').value = '';
      $('pin_nominal').value = '';
      $('pinNominalHint').textContent = 'Ketuk field di atas untuk membuka numpad';
      $('pinNominalHint').style.color = '#7B8DB0';

      $('pinJenisGroup').className = 'popup-form-group hidden';
      $('pinNominalGroup').className = 'popup-form-group hidden';
      $('pinAddMoreRow').className = 'popup-add-more-row hidden';

      setDis('btnSimpanPin', true);
      updateSimpanPinLabel();
    }
  });
};

// ===== TAMBAH PINJAMAN BARU (multi-entry) =====
window.tambahPinjamanBaru = () => {
  const nama = $('pin_nama').value;
  const jenis = $('pin_jenis').value;
  const nominal = parseInt($('pin_nominal').value) || 0;

  if (!nama || !jenis || nominal < 1) {
    showToast('⚠ Lengkapi semua field dahulu!');
    return;
  }

  _pinDraftList.push({ nama, jenis, nominal });
  renderPinDraftList();
  showToast('✅ Ditambahkan ke daftar');

  // Kunci Nama Pegawai (readonly) supaya tidak bisa diganti di tengah
  // sesi input — mencegah entri pinjaman lain "salah nempel" ke pegawai
  // yang berbeda dari yang sudah ada di daftar.
  $('pin_nama').disabled = true;
  $('pinNamaGroup').classList.add('popup-field-locked');
  $('btnGantiPegawaiPin').classList.remove('hidden');
  $('pinNamaLockedHint').classList.remove('hidden');

  // Jenis & nominal dikosongkan untuk entri berikutnya, dropdown jenis
  // difilter supaya jenis yang sudah dipakai pegawai ini tidak muncul lagi.
  populatePinJenisForNama(nama);
  $('pin_jenis').disabled = false;
  $('pin_jenis').style.color = '#7B8DB0';
  $('pin_nominal_display').value = '';
  $('pin_nominal').value = '';
  $('pinNominalHint').textContent = 'Ketuk field di atas untuk membuka numpad';
  $('pinNominalHint').style.color = '#7B8DB0';

  $('pinJenisGroup').className = 'popup-form-group visible';
  $('pinNominalGroup').className = 'popup-form-group hidden';
  $('pinAddMoreRow').className = 'popup-add-more-row hidden';

  setDis('btnSimpanPin', false);
  updateSimpanPinLabel();
  $('pin_jenis').focus();
};

window.hapusPinDraft = (idx) => {
  _pinDraftList.splice(idx, 1);
  renderPinDraftList();

  // Jika daftar jadi kosong, tidak ada lagi alasan mengunci nama
  // pegawai — buka kembali supaya admin bisa pilih pegawai lain.
  if (_pinDraftList.length === 0) {
    $('pin_nama').disabled = false;
    $('pinNamaGroup').classList.remove('popup-field-locked');
    $('btnGantiPegawaiPin').classList.add('hidden');
    $('pinNamaLockedHint').classList.add('hidden');
  } else {
    // Masih ada entri lain untuk nama yang sama: dropdown jenis perlu
    // disegarkan supaya jenis yang baru terhapus muncul lagi sebagai opsi.
    const namaAktif = $('pin_nama').value;
    if (namaAktif) populatePinJenisForNama(namaAktif);
  }

  const formOk = !!$('pin_jenis').value && (parseInt($('pin_nominal').value) || 0) > 0;
  setDis('btnSimpanPin', !formOk && _pinDraftList.length === 0);
  updateSimpanPinLabel();
};

window.updateSimpanPinLabel = () => {
  const btn = $('btnSimpanPin');
  if (!btn) return;
  const n = _pinDraftList.length;
  btn.innerHTML = n > 0
    ? '<i class="fa-solid fa-floppy-disk"></i><span>Simpan ' + n + ' Pinjaman</span>'
    : '<i class="fa-solid fa-floppy-disk"></i><span>Simpan Pinjaman</span>';
};

window.renderPinDraftList = () => {
  const wrap = $('pinDraftList');
  const totalWrap = $('pinDraftTotal');
  if (!wrap) return;

  if (_pinDraftList.length === 0) {
    wrap.innerHTML = '';
    if (totalWrap) totalWrap.className = 'pin-draft-total hidden';
    return;
  }

  wrap.innerHTML = _pinDraftList.map((p, i) => `
    <div class="pin-draft-item">
      <div class="pin-draft-item-icon"><i class="fa-solid fa-hand-holding-dollar"></i></div>
      <div class="pin-draft-item-info">
        <div class="pin-draft-item-name">${p.nama}</div>
        <div class="pin-draft-item-meta">${p.jenis}</div>
      </div>
      <div class="pin-draft-item-amount">Rp ${fmt(p.nominal)}</div>
      <button type="button" class="pin-draft-item-remove" onclick="hapusPinDraft(${i})">
        <i class="fa-solid fa-xmark"></i>
      </button>
    </div>
  `).join('');

  const total = _pinDraftList.reduce((s, p) => s + p.nominal, 0);
  if (totalWrap) {
    totalWrap.className = 'pin-draft-total';
    $('pinDraftTotalValue').textContent = 'Rp ' + fmt(total);
  }
};

window.simpanPinjaman = async () => {
  const nama = $('pin_nama').value;
  const jenis = $('pin_jenis').value;
  const nominal = parseInt($('pin_nominal').value) || 0;
  const formTerisiLengkap = !!nama && !!jenis && nominal >= 1;

  // Jika sudah ada entri di daftar TAPI form yang sedang diisi belum
  // ditekan "Tambah Pinjaman Baru", jangan diam-diam ikut menyimpannya.
  // Minta admin menyelesaikan entri tersebut dulu.
  if (_pinDraftList.length > 0 && formTerisiLengkap) {
    showToast('⚠ Tekan "Tambah Pinjaman Baru" dulu untuk menyelesaikan entri ini');
    const addBtn = document.getElementById('btnTambahPinjamanBaru');
    if (addBtn) {
      addBtn.classList.add('popup-btn-add-more-pulse');
      setTimeout(() => addBtn.classList.remove('popup-btn-add-more-pulse'), 900);
      addBtn.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
    return;
  }

  // Gabungkan draft list + form yang sedang diisi (kasus entri tunggal,
  // tanpa draft list sama sekali, boleh langsung simpan tanpa menekan Tambah)
  const items = [..._pinDraftList];
  if (formTerisiLengkap) {
    items.push({ nama, jenis, nominal });
  }

  if (items.length === 0) {
    showToast('⚠ Lengkapi semua field!');
    return;
  }
  
  const btn = $('btnSimpanPin');
  btn.innerHTML = '<i class="fa-solid fa-circle-notch fa-spin"></i><span>Menyimpan...</span>';
  btn.disabled = true;
  
  showLoading('Menyimpan...');
  
  try {
    const dataToInsert = items.map(item => ({
      id: _genId(),
      data: {
        tanggal: fmtTgl(new Date()),
        tanggalISO: new Date().toISOString().slice(0, 10),
        nama: item.nama,
        jenis: item.jenis,
        nominal: item.nominal,
        waktu: sbTimestamp()
      }
    }));
    const { error } = await sb.from(COL.pinjaman).insert(dataToInsert);
    if (error) throw error;

    _pinDraftList = [];
    btn.innerHTML='<i class="fa-solid fa-floppy-disk"></i><span>Simpan Pinjaman</span>';
    showToast(items.length > 1 ? `✅ ${items.length} pinjaman tersimpan!` : '✅ Pinjaman tersimpan!');
    tutupPopup('pinjamanPopup');
    
  } catch (e) {
    showToast('❌ Gagal: ' + e.message);
    updateSimpanPinLabel();
    btn.disabled = false;
  } finally {
    hideLoading();
  }
};

// ===== NUMPAD GENERAL (editNominalInput, ep_nominal, tamItem) =====

let _numpadGenValue = '';
let _numpadGenOnConfirm = null;

window.bukaNumpadGen = (label, initialVal, onConfirm) => {
  _numpadGenValue = initialVal ? String(initialVal) : '';
  _numpadGenOnConfirm = onConfirm;
  $('numpadGenLabel').textContent = label;
  $('numpadGenDisplay').textContent = _numpadGenValue ? fmt(parseInt(_numpadGenValue)) : '0';
  $('numpadGenOverlay').classList.add('active');
};

window.numpadInput = (digit) => {
  if (_numpadGenValue.length >= 12) return;
  _numpadGenValue += digit;
  $('numpadGenDisplay').textContent = _numpadGenValue ? fmt(parseInt(_numpadGenValue)) : '0';
};

window.numpadBackspace = () => {
  _numpadGenValue = _numpadGenValue.slice(0, -1);
  $('numpadGenDisplay').textContent = _numpadGenValue ? fmt(parseInt(_numpadGenValue)) : '0';
};

window.numpadGenConfirm = () => {
  const val = parseInt(_numpadGenValue) || 0;
  if (_numpadGenOnConfirm) _numpadGenOnConfirm(val);
  tutupNumpadGen();
};

window.tutupNumpadGen = () => {
  $('numpadGenOverlay').classList.remove('active');
  _numpadGenValue = '';
  _numpadGenOnConfirm = null;
};

// Trigger: Input Qty
window.bukaNumpadQty = (target) => {
  const initialVal = (target === 'input') ? ($('inp_qty').value || '') : ($('edit_qty_val').value || '');
  const label = (target === 'input') ? 'Qty' : 'Qty Baru';
  bukaNumpadGen(label, initialVal, (val) => {
    if (target === 'input') {
      $('inp_qty').value = val || '';
      $('inp_qty_display').value = val > 0 ? val : '';
      $('inp_qty_display').style.color = val > 0 ? '#1A2340' : '#7B8DB0';
      if (val < 1) { showToast('⚠ QTY minimal 1!'); }
      onInpQty();
    } else if (target === 'edit') {
      $('edit_qty_val').value = val || '';
      $('edit_qty_display').value = val > 0 ? val : '';
      bukaPopup('editQtyPopup');
    }
  });
};

// Trigger: Edit Nominal Bayar
window.bukaNumpadGenEditNominal = () => {
  const cur = parseInt($('editNominalInput').value) || 0;
  bukaNumpadGen('Nominal Bayar Baru', cur > 0 ? String(cur) : '', (val) => {
    $('editNominalInput').value = val;
    $('editNominalDisplay').value = val > 0 ? 'Rp ' + fmt(val) : '';
    $('editNominalHint').innerHTML = val > 0
      ? `Nominal baru: <b>Rp ${fmt(val)}</b><br><span style="color:var(--muted);">Input 0 untuk membatalkan pembayaran</span>`
      : `<span style="color:var(--muted);">Input 0 untuk membatalkan pembayaran</span>`;
  });
};

// Trigger: Pinjaman
window.bukaNumpadPinjaman = () => {
  const cur = parseInt($('pin_nominal').value) || 0;
  bukaNumpadGen('Jumlah Pinjaman', cur > 0 ? String(cur) : '', (val) => {
    $('pin_nominal').value = val;
    $('pin_nominal_display').value = val > 0 ? 'Rp ' + fmt(val) : '';
    cekSimpanPin();
  });
};

// Trigger: Edit Pinjaman nominal
window.bukaNumpadGenEpNominal = () => {
  const cur = parseInt(document.getElementById('ep_nominal').value) || 0;
  bukaNumpadGen('Nominal Pinjaman', cur > 0 ? String(cur) : '', (val) => {
    document.getElementById('ep_nominal').value = val;
    document.getElementById('ep_nominal_display').value = val > 0 ? 'Rp ' + fmt(val) : '';
  });
};

// Trigger: Bayar Pinjaman
window.bukaNumpadBayar = (index, maxVal) => {
  const cur = parseInt($('bayar_input_' + index).value) || 0;
  const jenis = _bayarJenisData[index]?.jenis || 'Bayar';
  bukaNumpadGen(jenis, cur > 0 ? String(cur) : '', (val) => {
    $('bayar_input_' + index).value = val;
    $('bayar_input_display_' + index).value = val > 0 ? 'Rp ' + fmt(val) : '';
    onBayarJenisInput(index, maxVal);
  });
};

// Trigger: Tambahan item qty
window.bukaNumpadGenTam = (idx) => {
  const items = window._tamCurrentItems || [];
  const t = items[idx];
  const satuan = (t && t.satuan) || 'HARI';
  const label = t ? `${t.jenis} (${satuan.toLowerCase()})` : 'Jumlah';
  const cur = window._tamValues[idx] || 0;
  bukaNumpadGen(label, cur > 0 ? String(cur) : '', (val) => {
    if(satuan.toUpperCase() === 'HARI' && val > 31) { val = 31; showToast('⚠ Maksimal 31 hari!'); }
    window._tamValues[idx] = val;
    const el = document.getElementById('tamItem_' + idx);
    if(el) el.value = val > 0 ? val + ' ' + satuan.toLowerCase() : '';
    hitungTotalTambahan();
  });
};

// ===== BAYAR PINJAMAN FUNCTIONS =====

window.bukaBayar = () => {
  const data = getRekapPinjamanLocal();
  _sisaPerNama = {};
  _nilaiPerNama = {};
  
  data.forEach(r => {
    if (r.sisa > 0) {
      _sisaPerNama[r.nama] = r.sisa;
      _nilaiPerNama[r.nama] = r.pinjaman;
    }
  });
  
  const list = Object.keys(_sisaPerNama);
  if (!list.length) {
    showToast('ℹ Tidak ada sisa pinjaman');
    return;
  }
  
  resetSelect('bayar_nama', list, '-- Pilih Pegawai --');
  $('bayar_nilai').value = '-';
  $('bayar_jenis_list').innerHTML = '';
  setDis('btnSimpanBayar', true);
  $('btnSimpanBayar').innerHTML = '<i class="fa-solid fa-check"></i><span>Simpan Pembayaran</span>';
  
  _bayarLuarGaji = false;
  const lgTrack = $('lgTrack');
  const lgThumb = $('lgThumb');
  if (lgTrack) lgTrack.classList.remove('lg-active');
  if (lgThumb) lgThumb.style.left = '2px';
  
  $('bayarPendapatanCard').style.display = 'none';
  $('bayarTotalGroup').style.display = 'none';
  
  const popup = $('bayarPopup');
  const sheet = popup.querySelector('.popup-sheet');
  if (sheet) { sheet.style.willChange = 'transform'; sheet.style.transform = 'translateY(100%)'; }
  popup.style.display = 'flex';
  if (sheet) {
    sheet.offsetHeight;
    requestAnimationFrame(() => {
      sheet.style.transform = 'translateY(0)';
      setTimeout(() => { sheet.style.willChange = 'auto'; }, 300);
    });
  }
};

window.tutupPopupBayar = () => {
  const popup = $('bayarPopup');
  const sheet = popup.querySelector('.popup-sheet');
  if (sheet) {
    sheet.style.willChange = 'transform';
    sheet.style.transform = 'translateY(100%)';
    setTimeout(() => {
      popup.style.display = 'none';
      sheet.style.transform = '';
      sheet.style.willChange = 'auto';
    }, 250);
  } else {
    popup.style.display = 'none';
  }
};

window.onBayarNama = () => {
  const nama = $('bayar_nama').value;
  $('bayar_nama').style.color = nama ? '#1A2340' : '#7B8DB0';
  
  if (!nama) {
    $('bayarPendapatanCard').style.display = 'none';
    $('bayarTotalGroup').style.display = 'none';
    $('bayar_jenis_list').innerHTML = '';
    setDis('btnSimpanBayar', true);
    return;
  }
  
  $('bayarPendapatanCard').style.display = 'flex';
  $('bayarTotalGroup').style.display = 'block';
  
  const dari = _getISO('rekapMulai');
  const sampai = _getISO('rekapAkhir');
  const res = getRincianLocal(nama, dari, sampai);
  const tam = _rcTambahan[nama];
  const tamTgl = tam?._tanggalISO || '';
  const tamDalamRange = !tamTgl || ((!dari || tamTgl >= dari) && (!sampai || tamTgl <= sampai));
  const tamTotalFromItems2 = (tam && tam.items) ? Object.values(tam.items).reduce((s, it) => s + (it.jumlah > 0 ? it.jumlah : (it.qty||0)*(it.harga||0)), 0) : 0;
  const tamTotal = tamDalamRange ? ((tam && tam.total > 0) ? tam.total : tamTotalFromItems2) : 0;
  
  const potMap = {};
  _CACHE.bayar.filter(b => {
    if (dari && b.tanggalISO < dari) return false;
    if (sampai && b.tanggalISO > sampai) return false;
    if (b.luarGaji) return false;
    return b.nama === nama;
  }).forEach(b => {
    potMap[b.jenisPinjaman] = (potMap[b.jenisPinjaman] || 0) + (b.bayar || 0);
  });
  
  const sudahPotong = Object.values(potMap).reduce((a, b) => a + b, 0);
  _bayarPendapatanBersih = Math.max(0, res.subtotal + tamTotal - sudahPotong);
  
  $('bayarPendapatanVal').textContent = fmt(_bayarPendapatanBersih);
  $('bayar_nilai').value = 'Rp ' + fmt(_sisaPerNama[nama] || 0);
  
  _bayarJenisData = getRekapPinjamanPerJenisLocal(nama);
  const container = $('bayar_jenis_list');
  
  container.innerHTML = _bayarJenisData.map((p, i) => `
    <div class="bayar-pinj-item">
      <div class="bayar-pinj-header">${p.jenis}</div>
      <div class="bayar-pinj-row">
        <div>
          <div class="bayar-pinj-label">Sisa Pinjaman</div>
          <input type="text" id="bayar_sisa_${i}" value="Rp ${fmt(p.sisa)}" readonly class="bayar-pinj-sisa">
        </div>
        <div>
          <div class="bayar-pinj-label">Bayar</div>
          <div class="popup-input-trigger" onclick="bukaNumpadBayar(${i}, ${p.sisa})">
            <input type="text" id="bayar_input_display_${i}" readonly placeholder="Ketuk..." 
              class="bayar-pinj-input-no-keyboard" inputmode="none">
            <input type="hidden" id="bayar_input_${i}" value="">
          </div>
        </div>
      </div>
      <button onclick="pelunasanPenuhNumpad(${i}, ${p.sisa})" class="bayar-pinj-lunas">
        <i class="fa-solid fa-circle-check"></i> Pelunasan Penuh
      </button>
      <div class="bayar-pinj-info" id="bayar_info_${i}">
        <span class="bayar-pinj-info-text">Sisa setelah bayar</span>
        <span class="bayar-pinj-info-val" id="bayar_sisa_hasil_${i}">-</span>
      </div>
    </div>
  `).join('');
  
  cekSimpanBayar();
};

window.pelunasanPenuhNumpad = (i, sisaAsli) => {
  let totalLain = 0;
  _bayarJenisData.forEach((p, j) => {
    if (j === i) return;
    totalLain += parseInt($('bayar_input_' + j)?.value) || 0;
  });
  
  const sisaPendapatan = Math.max(0, _bayarPendapatanBersih - totalLain);
  const maxBayar = _bayarLuarGaji ? sisaAsli : Math.min(sisaAsli, sisaPendapatan);
  
  $('bayar_input_' + i).value = maxBayar;
  $('bayar_input_display_' + i).value = maxBayar > 0 ? 'Rp ' + fmt(maxBayar) : '';
  onBayarJenisInput(i, sisaAsli);
  
  if (!_bayarLuarGaji && maxBayar < sisaAsli) {
    showToast('⚠ Dibatasi pendapatan: Rp ' + fmt(maxBayar));
  }
};

window.onBayarJenisInput = (i, sisaAsli) => {
  const val = parseInt($('bayar_input_' + i)?.value) || 0;
  
  const infoEl = $('bayar_info_' + i);
  const hasilEl = $('bayar_sisa_hasil_' + i);
  const sisaEl = $('bayar_sisa_' + i);
  
  let totalBayar = 0;
  _bayarJenisData.forEach((p, j) => {
    totalBayar += parseInt($('bayar_input_' + j)?.value) || 0;
  });
  
  const melebihiPendapatan = !_bayarLuarGaji && totalBayar > _bayarPendapatanBersih;
  const melebihiSisa = val > sisaAsli;
  
  if (val > 0) {
    infoEl.classList.add('show');
    const sisaSetelah = sisaAsli - val;
    
    if (melebihiSisa) {
      hasilEl.textContent = '⚠ Melebihi sisa!';
      hasilEl.className = 'bayar-pinj-info-val warning';
    } else if (melebihiPendapatan) {
      hasilEl.textContent = '⚠ Melebihi gaji!';
      hasilEl.className = 'bayar-pinj-info-val warning';
    } else if (sisaSetelah === 0) {
      hasilEl.textContent = 'LUNAS ✓';
      hasilEl.className = 'bayar-pinj-info-val lunas';
    } else {
      hasilEl.textContent = fmt(sisaSetelah);
      hasilEl.className = 'bayar-pinj-info-val';
    }
    
    sisaEl.style.color = (melebihiSisa || melebihiPendapatan) ? 'var(--red)' : 'var(--text)';
  } else {
    infoEl.classList.remove('show');
    sisaEl.style.color = 'var(--text)';
  }
  
  cekSimpanBayar();
};

function cekSimpanBayar() {
  let adaIsi = false, adaMelebihi = false;
  let totalBayar = 0;
  
  _bayarJenisData.forEach((p, i) => {
    const val = parseInt($('bayar_input_' + i)?.value) || 0;
    if (val > 0) adaIsi = true;
    if (val > p.sisa) adaMelebihi = true;
    totalBayar += val;
  });
  
  const melebihiPendapatan = !_bayarLuarGaji && totalBayar > _bayarPendapatanBersih;
  
  setDis('btnSimpanBayar', !adaIsi || adaMelebihi || melebihiPendapatan);
}

window.simpanBayar = async () => {
  const nama = $('bayar_nama').value;
  if (!nama) {
    showToast('Pilih pegawai!');
    return;
  }
  
  const toBayar = [];
  _bayarJenisData.forEach((p, i) => {
    const val = parseInt($('bayar_input_' + i)?.value) || 0;
    if (val > 0) toBayar.push({ jenis: p.jenis, sisa: p.sisa, bayar: val });
  });
  
  if (!toBayar.length) {
    showToast('Isi minimal satu jumlah bayar!');
    return;
  }
  
  const totalBayar = toBayar.reduce((s, b) => s + b.bayar, 0);
  if (!_bayarLuarGaji && totalBayar > _bayarPendapatanBersih) {
    showToast('⚠ Total bayar melebihi pendapatan!');
    return;
  }
  
  const btn = $('btnSimpanBayar');
  btn.innerHTML = '<i class="fa-solid fa-circle-notch fa-spin"></i><span>Menyimpan...</span>';
  btn.disabled = true;
  
  try {
    const dataToInsert = toBayar.map(b => ({
      id: _genId(),
      data: {
        tanggal: fmtTgl(new Date()),
        tanggalISO: new Date().toISOString().slice(0, 10),
        nama,
        jenisPinjaman: b.jenis,
        sisaSebelum: b.sisa,
        bayar: b.bayar,
        luarGaji: _bayarLuarGaji,
        waktu: sbTimestamp()
      }
    }));
    const { error } = await sb.from(COL.bayar).insert(dataToInsert);
    if (error) throw error;
    
    showToast('✅ Pembayaran tersimpan!');
    tutupPopupBayar();
    btn.innerHTML = '<i class="fa-solid fa-check"></i><span>Simpan Pembayaran</span>';
    btn.disabled = false;
    
    setTimeout(() => {
      muatRekapPinjaman();
      muatRekapPegawai();
    }, 1200);
    
  } catch (e) {
    showToast('❌ Gagal: ' + e.message);
    btn.innerHTML = '<i class="fa-solid fa-check"></i><span>Simpan Pembayaran</span>';
    btn.disabled = false;
  }
};

// ===== NUMPAD BAYAR =====

window.bukaNumpadBayar = (index, maxVal) => {
  _numpadBayarIndex = index;
  _numpadValue = $('bayar_input_' + index).value || '';
  _numpadMaxValue = maxVal;
  
  const jenis = _bayarJenisData[index]?.jenis || 'Bayar';
  $('numpadBayarLabel').textContent = jenis;
  $('numpadBayarDisplay').textContent = _numpadValue ? fmt(parseInt(_numpadValue)) : '0';
  
  $('numpadBayarOverlay').classList.add('active');
};

window.numpadBayarInput = (digit) => {
  if (_numpadValue.length >= 12) return;
  _numpadValue += digit;
  $('numpadBayarDisplay').textContent = fmt(parseInt(_numpadValue));
};

window.numpadBayarBackspace = () => {
  _numpadValue = _numpadValue.slice(0, -1);
  $('numpadBayarDisplay').textContent = _numpadValue ? fmt(parseInt(_numpadValue)) : '0';
};

window.numpadBayarConfirm = () => {
  const val = parseInt(_numpadValue) || 0;
  const index = _numpadBayarIndex;
  
  $('bayar_input_' + index).value = val;
  $('bayar_input_display_' + index).value = val > 0 ? 'Rp ' + fmt(val) : '';
  
  const sisaAsli = _bayarJenisData[index]?.sisa || 0;
  onBayarJenisInput(index, sisaAsli);
  
  tutupNumpadBayar();
};

window.tutupNumpadBayar = () => {
  $('numpadBayarOverlay').classList.remove('active');
  _numpadValue = '';
  _numpadMaxValue = null;
  _numpadBayarIndex = null;
};
// ===== SETTING =====
window.openSetting = () => {
  initSettingPegawai();
  $('settingPage').classList.add('active');
  history.pushState({ page: 'settingPage' }, '');
  setTimeout(_loadPrintMarginsToUI, 50);
};
window.tutupSetting = () => { $('settingPage').classList.remove('active'); if (history.state && history.state.page === 'settingPage') history.back(); if(_dashboardStale) { _dashboardStale = false; loadDashboardDana(); } };

function initSettingPegawai() {
  _pegMode = 'select';
  const dd = $('set_peg_dropdown');
  dd.innerHTML = '<option value="">dropdown...</option>';
  MASTER.pegawai.forEach(p => dd.innerHTML += `<option value="${p}">${p}</option>`);
  dd.value = '';
  $('set_peg_bagian_ro').textContent = '—';
  _applyPegMode();
}

function _applyPegMode() {
  const isSelect = _pegMode === 'select', isEdit = _pegMode === 'edit', isTambah = _pegMode === 'tambah';
  $('set_peg_dropdown').style.display = isTambah ? 'none' : '';
  $('set_peg_nama').style.display = isTambah ? '' : 'none';
  $('btnTambahPeg').style.display = isTambah ? 'none' : 'flex';
  $('btnBatalTambahPeg').style.display = isTambah ? 'flex' : 'none';
  $('set_peg_bagian_ro').style.display = isSelect ? 'flex' : 'none';
  $('set_peg_bagian').style.display = isSelect ? 'none' : '';
  $('btnEditPeg').style.display = isSelect ? 'flex' : 'none';
  $('spacerEditPeg').style.display = (isEdit || isTambah) ? 'block' : 'none';
  $('pegRowHapusSave').style.display = isSelect ? 'none' : (isEdit ? 'grid' : 'none');
  $('btnSimpanPegawaiBaru').style.display = isTambah ? '' : 'none';
  if(isSelect) { const ada = !!$('set_peg_dropdown').value; $('pegRowHapusSave').style.display = ada ? 'grid' : 'none'; setDis('btnHapusPegawai', !ada); setDis('btnSimpanPegawai', true); }
}

window.onPilihPegawai = () => {
  const nama = $('set_peg_dropdown').value;
  if(!nama) { $('set_peg_bagian_ro').textContent = '—'; $('pegRowHapusSave').style.display = 'none'; return; }
  const d = MASTER.pegawaiData[nama] || {};
  $('set_peg_bagian_ro').textContent = d.bagian || '—';
  _pegMode = 'select';
  _applyPegMode();
};

window.modeTambahPeg = () => {
  _pegMode = 'tambah';
  $('set_peg_nama').value = '';
  resetSelect('set_peg_bagian', MASTER.bagian, '-- Pilih Bagian --', false);
  setDis('btnSimpanPegawaiBaru', true);
  _applyPegMode();
  setTimeout(() => $('set_peg_nama').focus(), 80);
};

window.modeSelectPeg = () => { _pegMode = 'select'; $('set_peg_dropdown').value = ''; $('set_peg_bagian_ro').textContent = '—'; _applyPegMode(); };

window.modeEditPeg = () => {
  const nama = $('set_peg_dropdown').value;
  if(!nama) return;
  const d = MASTER.pegawaiData[nama] || {};
  _pegMode = 'edit';
  resetSelect('set_peg_bagian', MASTER.bagian, '-- Pilih Bagian --', false);
  $('set_peg_bagian').value = d.bagian || '';
  setDis('btnSimpanPegawai', false);
  _applyPegMode();
};

window.onSetPegBagian = () => { cekSimpanPegawai(); };
window.cekSimpanPegawai = () => {
  if(_pegMode === 'tambah') { const ok = $('set_peg_nama').value.trim() && $('set_peg_bagian').value; setDis('btnSimpanPegawaiBaru', !ok); }
  else if(_pegMode === 'edit') { setDis('btnSimpanPegawai', !$('set_peg_bagian').value); }
};

window.simpanPegawai = async () => {
  if(_pegMode === 'tambah') {
    const nama = $('set_peg_nama').value.trim().toUpperCase();
    const bagian = $('set_peg_bagian').value;
    if(!nama || !bagian){ showToast('⚠ Nama & Bagian wajib diisi!'); return; }
    if(MASTER.pegawai.includes(nama)){ showToast('⚠ Pegawai sudah ada!'); return; }
    showLoading('Menyimpan...');
    try {
      await sbInsert(sbTable(db, COL.master), { type:'pegawai', nama, bagian, subbagian:'', waktu:sbTimestamp() });
      showToast('✅ Pegawai ditambahkan!');
      const dd = $('set_peg_dropdown');
      const opt = document.createElement('option'); opt.value = nama; opt.textContent = nama;
      const setelah = [...dd.options].find(o => o.value && o.value > nama);
      setelah ? dd.insertBefore(opt, setelah) : dd.appendChild(opt);
      dd.value = ''; $('set_peg_nama').value = ''; $('set_peg_bagian_ro').textContent = '—';
      _pegMode = 'select'; _applyPegMode();
    } catch(e){ showToast('❌ Gagal: '+e.message); }
    finally { hideLoading(); }
  } else if(_pegMode === 'edit') {
    const nama = $('set_peg_dropdown').value;
    const bagian = $('set_peg_bagian').value;
    const subbagian = (MASTER.pegawaiData[nama] && MASTER.pegawaiData[nama].subbagian) ? MASTER.pegawaiData[nama].subbagian : '';
    if(!nama || !bagian){ showToast('⚠ Pilih pegawai & bagian!'); return; }
    const docId = _masterDocIds['pegawai|'+nama];
    if(!docId){ showToast('❌ Data tidak ditemukan!'); return; }
    showLoading('Menyimpan...');
    try { await sbUpdate(sbDoc(db, COL.master, docId), { bagian, subbagian }); showToast('✅ Data pegawai diupdate!'); $('set_peg_dropdown').value = ''; $('set_peg_bagian_ro').textContent = '—'; _pegMode = 'select'; _applyPegMode(); }
    catch(e){ showToast('❌ Gagal: '+e.message); }
    finally { hideLoading(); }
  }
};

window.clearFormPegawai = () => { _pegMode = 'select'; $('set_peg_dropdown').value = ''; $('set_peg_nama').value = ''; $('set_peg_bagian_ro').textContent = '—'; _applyPegMode(); };

window.hapusPegawai = async () => {
  const nama = $('set_peg_dropdown').value;
  if(!nama){ showToast('⚠ Pilih pegawai dulu!'); return; }
  const docId = _masterDocIds['pegawai|'+nama];
  if(!docId){ showToast('❌ Data tidak ditemukan!'); return; }
  bukaConfirm({
    icon: '<i class="fa-solid fa-user-xmark"></i>',
    iconBg: '#FEE2E2', iconColor: '#EF4444',
    title: 'Hapus ' + nama + '?',
    msg: 'Semua data ' + nama + ' akan dihapus: transaksi, pinjaman, bayar, dan tambahan. Grafik historis tetap aman.',
    okLabel: 'Ya, Hapus Sekarang',
    okBg: 'linear-gradient(135deg,#DC2626,#EF4444)',
    callback: async () => {
      showLoading('Menghapus data ' + nama + '...');
      try {
        const allDels = [];
        allDels.push(sbDelete(sbDoc(db, COL.master, docId)));
        const inputSnap = await sbGetAll(sbTable(db, COL.input)); inputSnap.forEach(d => { if(d.data().nama === nama) allDels.push(sbDelete(sbDoc(db, COL.input, d.id))); });
        const pinSnap = await sbGetAll(sbTable(db, COL.pinjaman)); pinSnap.forEach(d => { if(d.data().nama === nama) allDels.push(sbDelete(sbDoc(db, COL.pinjaman, d.id))); });
        const bayarSnap = await sbGetAll(sbTable(db, COL.bayar)); bayarSnap.forEach(d => { if(d.data().nama === nama) allDels.push(sbDelete(sbDoc(db, COL.bayar, d.id))); });
        const tamSnap = await sbGetAll(sbTable(db, 'tambahan')); tamSnap.forEach(d => { if(d.data().nama === nama) allDels.push(sbDelete(sbDoc(db, 'tambahan', d.id))); });
        await Promise.all(allDels);
        _CACHE.input = _CACHE.input.filter(r => r.nama !== nama);
        _CACHE.pinjaman = _CACHE.pinjaman.filter(r => r.nama !== nama);
        _CACHE.bayar = _CACHE.bayar.filter(r => r.nama !== nama);
        delete _rcTambahan[nama];
        _saveTambahanToSession();
        MASTER.pegawai = MASTER.pegawai.filter(p => p !== nama);
        delete MASTER.pegawaiData[nama];
        delete _masterDocIds['pegawai|' + nama];
        const dd = $('set_peg_dropdown'); const opt = dd.querySelector(`option[value="${nama}"]`); if(opt) opt.remove();
        dd.value = ''; $('set_peg_bagian_ro').textContent = '—'; $('pegRowHapusSave').style.display = 'none';
        _pegMode = 'select'; _applyPegMode();
        showToast('✅ Semua data ' + nama + ' berhasil dihapus!');
        setTimeout(() => { muatRekapPegawai(); muatRekapPinjaman(); loadDashboardDana(); }, 400);
      } catch(e) { showToast('❌ Gagal: ' + e.message); }
      finally { hideLoading(); }
    }
  });
};

// ===== HARGA =====
window.bukaHarga = () => {
  initHargaPage();
  $('hargaPage').classList.add('active');
  history.pushState({ page: 'hargaPage' }, '');
};
window.tutupHarga = () => { $('hargaPage').classList.remove('active'); if (history.state && history.state.page === 'hargaPage') history.back(); if(_dashboardStale) { _dashboardStale = false; loadDashboardDana(); } };

function _populateHrgBrandSelect() {
  const sel = $('hrg_brand');
  if(!sel) return;
  sel.innerHTML = '<option value="">--- PILIH ---</option>';
  (MASTER.brands||[]).sort().forEach(b => sel.innerHTML += `<option value="${b}">${b}</option>`);
  sel.value = '';
}

function initHargaPage() {
  _populateHrgBrandSelect();
  const selBag = $('hrg_bagian');
  if(selBag) { selBag.innerHTML = '<option value="">--- PILIH ---</option>'; selBag.disabled = true; selBag.value = ''; }
  const cont = $('hrg_rows_container');
  if(cont) cont.innerHTML = `<div class="hrg-row" style="display:grid;grid-template-columns:1fr 1fr auto;gap:8px;align-items:center;margin-bottom:8px;">
    <input type="text" class="hrg-pekerjaan-inp" placeholder="Nama pekerjaan..." oninput="this.value=this.value.toUpperCase();cekSimpanHargaBaru()" autocomplete="off" autocorrect="off" autocapitalize="characters" spellcheck="false" style="padding:11px 12px;border:1.5px solid var(--muted);border-radius:12px;background:var(--card2);font-size:13px;font-family:inherit;color:var(--text);outline:none;width:100%;text-transform:uppercase;">
    <input type="number" class="hrg-harga-inp" placeholder="Harga (Rp)" inputmode="numeric" oninput="cekSimpanHargaBaru()" style="padding:11px 12px;border:1.5px solid var(--muted);border-radius:12px;background:var(--card2);font-size:13px;font-family:inherit;color:var(--text);outline:none;width:100%;">
    <button onclick="hapusHrgRow(this)" style="width:36px;height:36px;border-radius:10px;border:1.5px solid var(--muted);background:var(--card2);color:var(--muted);font-size:14px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;"><i class="fa-solid fa-xmark"></i></button>
  </div>`;
  setDis('btnSimpanSemuaHarga', true);
  _populateTamBagianSelect();
  _renderTamDaftarBagian('');
}

window.onHrgBrand = () => {
  const brand = $('hrg_brand').value;
  const selBag = $('hrg_bagian');
  if(selBag) { selBag.innerHTML = '<option value="">--- PILIH ---</option>'; if(!brand) { selBag.disabled = true; } else { selBag.disabled = false; (MASTER.bagian||[]).sort().forEach(b => selBag.innerHTML += `<option value="${b}">${b}</option>`); } selBag.value = ''; }
  cekSimpanHargaBaru();
  renderDaftarHarga();
};

window.onHrgBagian = () => { cekSimpanHargaBaru(); renderDaftarHarga(); };

window.hapusHrgRow = (btn) => {
  const rows = document.querySelectorAll('#hrg_rows_container .hrg-row');
  if(rows.length > 1) btn.closest('.hrg-row').remove();
  cekSimpanHargaBaru();
};

window.tambahHrgRow = () => {
  const cont = $('hrg_rows_container');
  const div = document.createElement('div');
  div.innerHTML = `<div class="hrg-row" style="display:grid;grid-template-columns:1fr 1fr auto;gap:8px;align-items:center;margin-bottom:8px;">
    <input type="text" class="hrg-pekerjaan-inp" placeholder="Nama pekerjaan..." oninput="this.value=this.value.toUpperCase();cekSimpanHargaBaru()" autocomplete="off" autocorrect="off" autocapitalize="characters" spellcheck="false" style="padding:11px 12px;border:1.5px solid var(--muted);border-radius:12px;background:var(--card2);font-size:13px;font-family:inherit;color:var(--text);outline:none;width:100%;text-transform:uppercase;">
    <input type="number" class="hrg-harga-inp" placeholder="Harga (Rp)" inputmode="numeric" oninput="cekSimpanHargaBaru()" style="padding:11px 12px;border:1.5px solid var(--muted);border-radius:12px;background:var(--card2);font-size:13px;font-family:inherit;color:var(--text);outline:none;width:100%;">
    <button onclick="hapusHrgRow(this)" style="width:36px;height:36px;border-radius:10px;border:1.5px solid var(--muted);background:var(--card2);color:var(--muted);font-size:14px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;"><i class="fa-solid fa-xmark"></i></button>
  </div>`;
  cont.appendChild(div.firstElementChild);
};

window.cekSimpanHargaBaru = () => {
  const brand = $('hrg_brand').value;
  let ada = false;
  document.querySelectorAll('#hrg_rows_container .hrg-row').forEach(row => {
    const pek = row.querySelector('.hrg-pekerjaan-inp')?.value.trim();
    const hrg = parseInt(row.querySelector('.hrg-harga-inp')?.value)||0;
    if(pek && hrg > 0) ada = true;
  });
  setDis('btnSimpanSemuaHarga', !(brand && ada));
};

window.simpanSemuaHarga = async () => {
  const brand = $('hrg_brand').value;
  const bagian = $('hrg_bagian').value;
  if(!brand){ showToast('⚠ Pilih Brand terlebih dahulu!'); return; }
  const rows = [];
  document.querySelectorAll('#hrg_rows_container .hrg-row').forEach(row => {
    const pek = row.querySelector('.hrg-pekerjaan-inp')?.value.trim();
    const hrg = parseInt(row.querySelector('.hrg-harga-inp')?.value)||0;
    if(pek && hrg > 0) rows.push({ pekerjaan:pek, harga:hrg });
  });
  if(!rows.length){ showToast('⚠ Isi minimal satu pekerjaan & harga!'); return; }
  showLoading('Menyimpan harga...');
  try {
    for(const r of rows) {
      const key = brand + '|' + r.pekerjaan;
      if(_hargaDocIds[key]) {
        await sbUpdate(sbDoc(db, COL.harga, _hargaDocIds[key]), { brand, bagian, pekerjaan:r.pekerjaan, harga:r.harga });
        // Update MASTER.hargaList in-place
        const idx = MASTER.hargaList.findIndex(h => h.id === _hargaDocIds[key]);
        if(idx >= 0) { MASTER.hargaList[idx].harga = r.harga; MASTER.hargaList[idx].bagian = bagian; }
      } else {
        const res = await sbInsert(sbTable(db, COL.harga), { brand, bagian, pekerjaan:r.pekerjaan, harga:r.harga, waktu:sbTimestamp() });
        // Tambah ke MASTER.hargaList dan _hargaDocIds
        _hargaDocIds[key] = res.id;
        MASTER.hargaList.push({ id:res.id, brand, bagian, pekerjaan:r.pekerjaan, harga:r.harga });
      }
      MASTER.harga[key] = r.harga;
    }
    showToast('✅ Semua harga disimpan!');
    $('hrg_brand').value = '';
    const selBag = $('hrg_bagian'); if(selBag) { selBag.innerHTML = '<option value="">--- PILIH ---</option>'; selBag.disabled = true; selBag.value = ''; }
    const cont = $('hrg_rows_container');
    if(cont) cont.innerHTML = `<div class="hrg-row" style="display:grid;grid-template-columns:1fr 1fr auto;gap:8px;align-items:center;margin-bottom:8px;">
      <input type="text" class="hrg-pekerjaan-inp" placeholder="Nama pekerjaan..." oninput="this.value=this.value.toUpperCase();cekSimpanHargaBaru()" autocomplete="off" autocorrect="off" autocapitalize="characters" spellcheck="false" style="padding:11px 12px;border:1.5px solid var(--muted);border-radius:12px;background:var(--card2);font-size:13px;font-family:inherit;color:var(--text);outline:none;width:100%;text-transform:uppercase;">
      <input type="number" class="hrg-harga-inp" placeholder="Harga (Rp)" inputmode="numeric" oninput="cekSimpanHargaBaru()" style="padding:11px 12px;border:1.5px solid var(--muted);border-radius:12px;background:var(--card2);font-size:13px;font-family:inherit;color:var(--text);outline:none;width:100%;">
      <button onclick="hapusHrgRow(this)" style="width:36px;height:36px;border-radius:10px;border:1.5px solid var(--muted);background:var(--card2);color:var(--muted);font-size:14px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;"><i class="fa-solid fa-xmark"></i></button>
    </div>`;
    setDis('btnSimpanSemuaHarga', true);
    renderDaftarHarga();
  } catch(e){ showToast('❌ Gagal: '+e.message); }
  finally { hideLoading(); }
};

window.toggleDaftarHarga = () => {
  const body = $('daftarHrgBody');
  const chev = $('daftarHrgChevron');
  if(body.style.display === 'none') { body.style.display = 'block'; chev.style.transform = 'rotate(180deg)'; renderDaftarHarga(); }
  else { body.style.display = 'none'; chev.style.transform = ''; }
};

function renderDaftarHarga() {
  const body = $('daftarHrgBody');
  if(!body || body.style.display === 'none') return;
  const brand = $('hrg_brand').value, bagian = $('hrg_bagian').value;
  if(!brand && !bagian) {
    body.innerHTML = `<div style="text-align:center;color:var(--muted);font-size:12px;padding:16px;background:var(--card2);border-radius:12px;"><i class="fa-solid fa-filter" style="display:block;font-size:20px;margin-bottom:8px;opacity:.4;"></i>Pilih Brand dan Bagian untuk melihat daftar harga</div>`;
    return;
  }
  const list = MASTER.hargaList.filter(h => { if(brand && h.brand !== brand) return false; if(bagian && h.bagian !== bagian) return false; return true; });
  if(!list.length){
    body.innerHTML = `<div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;margin-bottom:8px;padding:0 2px;">${brand}${bagian?' — '+bagian:''}</div><div style="text-align:center;color:var(--muted);font-size:12px;padding:12px;background:var(--card2);border-radius:12px;">Belum ada data harga untuk filter ini</div>`;
    return;
  }
  const grouped = {};
  list.forEach(h => { const key = h.bagian || '(Tanpa Bagian)'; if(!grouped[key]) grouped[key] = []; grouped[key].push(h); });
  const bagianKeys = Object.keys(grouped).sort();
  let html = `<div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;margin-bottom:10px;padding:0 2px;">${brand}${bagian?' — '+bagian:''} &mdash; ${list.length} data</div>`;
  bagianKeys.forEach(bag => {
    html += `<div style="margin-bottom:14px;"><div style="font-size:10px;font-weight:800;letter-spacing:.09em;text-transform:uppercase;color:var(--primary3);padding:6px 10px;background:var(--primaryDim);border-radius:8px;margin-bottom:6px;">${bag}</div>`;
    grouped[bag].forEach(h => {
      const hid = h.id, hpek = (h.pekerjaan||'-').replace(/`/g,"'"), hhrg = h.harga||0;
      html += `<div id="hrgItem_${hid}" style="background:var(--card2);border-radius:12px;padding:8px 10px;margin-bottom:6px;border:1px solid var(--muted);">
        <div style="display:flex;align-items:center;gap:6px;">
          <div class="hrgView_${hid}" style="flex:1;display:flex;justify-content:space-between;align-items:center;gap:6px;">
            <div style="font-size:13px;font-weight:700;color:var(--text);flex:1;">${hpek}</div>
            <div style="font-size:13px;font-weight:800;color:var(--text);white-space:nowrap;">Rp ${fmt(hhrg)}</div>
          </div>
          <div class="hrgEditForm_${hid}" style="flex:1;display:none;gap:5px;align-items:center;">
            <input id="hrgEditNama_${hid}" type="text" value="${hpek}" style="flex:1;padding:6px 8px;border:1.5px solid var(--primary);border-radius:9px;background:#fff;font-size:12px;font-family:inherit;color:var(--text);outline:none;min-width:0;">
            <input id="hrgEditHarga_${hid}" type="number" value="${hhrg}" inputmode="numeric" style="width:75px;padding:6px 7px;border:1.5px solid var(--primary);border-radius:9px;background:#fff;font-size:12px;font-family:inherit;color:var(--text);outline:none;">
          </div>
          <div style="display:flex;gap:4px;flex-shrink:0;">
            <button id="hrgBtnEdit_${hid}" onclick="window.toggleEditHargaItem('${hid}')" style="width:28px;height:28px;border-radius:7px;border:1.5px solid var(--primary);background:var(--primaryDim);color:var(--primary3);font-size:11px;cursor:pointer;display:flex;align-items:center;justify-content:center;"><i class='fa-solid fa-pencil'></i></button>
            <button id="hrgBtnSave_${hid}" onclick="window.simpanEditHargaItem('${hid}')" style="width:28px;height:28px;border-radius:7px;border:none;background:linear-gradient(135deg,var(--primary3),var(--primary));color:#fff;font-size:11px;cursor:pointer;display:none;align-items:center;justify-content:center;"><i class='fa-solid fa-check'></i></button>
            <button onclick="window.hapusHargaItem('${hid}','${hpek}')" style="width:28px;height:28px;border-radius:7px;border:1.5px solid var(--red);background:#FEE2E2;color:var(--red);font-size:11px;cursor:pointer;display:flex;align-items:center;justify-content:center;"><i class='fa-solid fa-trash'></i></button>
          </div>
        </div>
      </div>`;
    });
    html += `</div>`;
  });
  body.innerHTML = html;
}

window.toggleEditHargaItem = (id) => {
  const view = document.querySelector('.hrgView_' + id);
  const form = document.querySelector('.hrgEditForm_' + id);
  const btnEdit = document.getElementById('hrgBtnEdit_' + id);
  const btnSave = document.getElementById('hrgBtnSave_' + id);
  if(!view || !form) return;
  const isEditing = form.style.display !== 'none';
  if(isEditing) { form.style.display = 'none'; view.style.display = 'flex'; btnEdit.style.display = 'flex'; btnSave.style.display = 'none'; }
  else { view.style.display = 'none'; form.style.display = 'flex'; btnEdit.style.display = 'none'; btnSave.style.display = 'flex'; const inp = document.getElementById('hrgEditNama_' + id); if(inp) setTimeout(() => inp.focus(), 50); }
};

window.simpanEditHargaItem = async (id) => {
  const namaInp = document.getElementById('hrgEditNama_' + id);
  const hargaInp = document.getElementById('hrgEditHarga_' + id);
  if(!namaInp || !hargaInp) return;
  const newNama = namaInp.value.trim().toUpperCase();
  const newHarga = parseInt(hargaInp.value) || 0;
  if(!newNama || newHarga <= 0) { showToast('⚠ Nama & harga tidak boleh kosong!'); return; }
  showLoading('Menyimpan...');
  try {
    const item = MASTER.hargaList.find(h => h.id === id);
    if(!item) { showToast('❌ Data tidak ditemukan!'); return; }
    await sbUpdate(sbDoc(db, COL.harga, id), { pekerjaan: newNama, harga: newHarga });
    // Update MASTER in-place
    const oldKey = item.brand + '|' + item.pekerjaan;
    const newKey = item.brand + '|' + newNama;
    item.pekerjaan = newNama;
    item.harga = newHarga;
    delete MASTER.harga[oldKey];
    MASTER.harga[newKey] = newHarga;
    if(_hargaDocIds[oldKey]) { delete _hargaDocIds[oldKey]; _hargaDocIds[newKey] = id; }
    showToast('✅ Harga berhasil diupdate!');
    renderDaftarHarga();
  } catch(e) { showToast('❌ Gagal: ' + e.message); }
  finally { hideLoading(); }
};

window.hapusHargaItem = async (id, pekerjaan) => {
  bukaConfirm({
    icon: '<i class="fa-solid fa-trash"></i>',
    iconBg: '#FEE2E2', iconColor: '#EF4444',
    title: 'Hapus Harga?',
    msg: `"${pekerjaan}" akan dihapus permanen dan tidak bisa dikembalikan.`,
    okLabel: 'Ya, Hapus',
    okBg: 'linear-gradient(135deg,#DC2626,#EF4444)',
    callback: () => {
      MASTER.hargaList = MASTER.hargaList.filter(h => h.id !== id);
      renderDaftarHarga();
      showToast('🗑 Harga berhasil dihapus!');
      sbDelete(sbDoc(db, COL.harga, id)).catch(e => showToast('❌ Gagal hapus: ' + e.message));
    }
  });
};

// ===== TAMBAHAN =====
const TAM_DEFAULT = ['LEMBUR', 'UANG MAKAN', 'BONUS'];

function _populateTamBagianSelect() {
  const sel = $('tam_bagian_sel');
  if(!sel) return;
  const uniqueBagian = [...new Set((MASTER.bagian||[]).concat(MASTER.hargaList.filter(h=>h.bagian).map(h=>h.bagian), (MASTER.tambahanItemList||[]).filter(t=>t.bagian).map(t=>t.bagian)))].filter(Boolean).sort();
  sel.innerHTML = '<option value="">--- PILIH BAGIAN ---</option>';
  uniqueBagian.forEach(b => sel.innerHTML += `<option value="${b}">${b}</option>`);
  sel.value = '';
}

function _renderTamDaftarBagian(bagian) {
  const cont = $('tam_daftar_bagian');
  const btnTambah = $('btnTamTambahItem');
  const formBaru = $('tam_form_baru');
  if(!cont) return;
  if(!bagian) { cont.innerHTML = ''; if(btnTambah) btnTambah.style.display = 'none'; if(formBaru) formBaru.style.display = 'none'; return; }
  const items = (MASTER.tambahanItemList||[]).filter(t => t.bagian === bagian);
  const existingJenis = items.map(t => t.jenis);
  const defaultMissing = TAM_DEFAULT.filter(j => !existingJenis.includes(j));
  const _satOpts = (sel) => ['HARI','PCS','JAM'].map(s => `<option value="${s}" ${s===sel?'selected':''}>${s}</option>`).join('');
  let html = '';
  items.sort((a,b) => a.jenis.localeCompare(b.jenis)).forEach(t => {
    html += `<div style="display:grid;grid-template-columns:1fr 90px 64px auto;gap:6px;align-items:center;margin-bottom:8px;" data-tam-id="${t.id||''}" data-tam-jenis="${t.jenis}">
      <div style="padding:11px 12px;border:1.5px solid var(--muted);border-radius:12px;background:var(--bg2);font-size:13px;font-family:inherit;font-weight:700;color:var(--text2);overflow:hidden;text-overflow:ellipsis;">${t.jenis}</div>
      <input type="number" class="tam-edit-harga" value="${t.harga||''}" placeholder="Rp" inputmode="numeric" onchange="simpanEditTambahan(this, '${bagian}', '${t.jenis}')" style="width:90px;padding:11px 8px;border:1.5px solid var(--muted);border-radius:12px;background:var(--card2);font-size:13px;font-family:inherit;color:var(--text);outline:none;text-align:right;">
      <select class="tam-edit-satuan" onchange="simpanSatuanTambahan(this, '${bagian}', '${t.jenis}')" style="width:64px;padding:11px 4px;border:1.5px solid var(--muted);border-radius:12px;background:var(--card2);font-size:11px;font-family:inherit;font-weight:700;color:var(--text);outline:none;appearance:none;-webkit-appearance:none;text-align:center;">${_satOpts(t.satuan||'HARI')}</select>
      <button onclick="hapusTambahan('${bagian}','${t.jenis}')" style="width:38px;height:42px;border-radius:12px;border:1.5px solid var(--red);background:transparent;color:var(--red);font-size:13px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;"><i class="fa-solid fa-trash"></i></button>
    </div>`;
  });
  defaultMissing.forEach(jenis => {
    html += `<div style="display:grid;grid-template-columns:1fr 90px 64px auto;gap:6px;align-items:center;margin-bottom:8px;">
      <div style="padding:11px 12px;border:1.5px solid var(--muted);border-radius:12px;background:var(--bg2);font-size:13px;font-family:inherit;font-weight:700;color:var(--muted);overflow:hidden;text-overflow:ellipsis;">${jenis}</div>
      <input type="number" class="tam-edit-harga" placeholder="Rp" inputmode="numeric" onchange="simpanEditTambahan(this, '${bagian}', '${jenis}')" style="width:90px;padding:11px 8px;border:1.5px dashed var(--muted);border-radius:12px;background:var(--card2);font-size:13px;font-family:inherit;color:var(--text);outline:none;text-align:right;">
      <select class="tam-edit-satuan" disabled style="width:64px;padding:11px 4px;border:1.5px dashed var(--muted);border-radius:12px;background:var(--card2);font-size:11px;font-family:inherit;font-weight:700;color:var(--muted);outline:none;appearance:none;-webkit-appearance:none;text-align:center;">${_satOpts('HARI')}</select>
      <div style="width:38px;height:42px;"></div>
    </div>`;
  });
  cont.innerHTML = html || `<div style="text-align:center;color:var(--muted);font-size:12px;padding:12px;background:var(--card2);border-radius:12px;margin-bottom:8px;">Belum ada item tambahan untuk <b>${bagian}</b></div>`;
  if(btnTambah) btnTambah.style.display = 'block';
}

window.onTamBagianChange = () => {
  const bagian = $('tam_bagian_sel').value;
  _renderTamDaftarBagian(bagian);
  const fb = $('tam_form_baru'); if(fb) fb.style.display = 'none';
  renderDaftarTambahan();
};

window.toggleTamFormBaru = () => {
  const fb = $('tam_form_baru');
  if(!fb) return;
  const open = fb.style.display !== 'none';
  fb.style.display = open ? 'none' : 'block';
  const btn = $('btnTamTambahItem');
  if(btn) btn.textContent = open ? '+ Tambah Item Lain' : '✕ Batal';
  if(!open) { const ni=$('tam_new_nama'); const hi=$('tam_new_harga'); if(ni)ni.value=''; if(hi)hi.value=''; cekSimpanTambahanBaru(); }
};

window.cekSimpanTambahanBaru = () => {
  const nama = $('tam_new_nama')?.value.trim();
  const hrg = parseInt($('tam_new_harga')?.value)||0;
  setDis('btnTamSimpanBaru', !(nama && hrg > 0));
};

window.simpanEditTambahan = async (inp, bagian, jenis) => {
  const harga = parseInt(inp.value)||0;
  if(!harga){ inp.value=''; return; }
  try {
    const key = 'tambahan_item|' + bagian + '|' + jenis;
    if(_hargaDocIds[key]) { await sbUpdate(sbDoc(db, COL.harga, _hargaDocIds[key]), { harga }); }
    else { const ref = await sbInsert(sbTable(db, COL.harga), { type:'tambahan_item', bagian, jenis, harga, satuan:'HARI', waktu:sbTimestamp() }); _hargaDocIds[key] = ref.id; if(!MASTER.tambahanItemList) MASTER.tambahanItemList = []; MASTER.tambahanItemList.push({ id:ref.id, bagian, jenis, harga, satuan:'HARI' }); }
    const idx = (MASTER.tambahanItemList||[]).findIndex(t=>t.bagian===bagian&&t.jenis===jenis);
    if(idx>=0) MASTER.tambahanItemList[idx].harga = harga;
    showToast('✅ Harga disimpan!');
    renderDaftarTambahan();
  } catch(e){ showToast('❌ Gagal: '+e.message); }
};

window.simpanSatuanTambahan = async (sel, bagian, jenis) => {
  const satuan = sel.value || 'HARI';
  try {
    const key = 'tambahan_item|' + bagian + '|' + jenis;
    if(_hargaDocIds[key]) { await sbUpdate(sbDoc(db, COL.harga, _hargaDocIds[key]), { satuan }); }
    const idx = (MASTER.tambahanItemList||[]).findIndex(t=>t.bagian===bagian&&t.jenis===jenis);
    if(idx>=0) MASTER.tambahanItemList[idx].satuan = satuan;
    showToast('✅ Satuan disimpan!');
    renderDaftarTambahan();
  } catch(e){ showToast('❌ Gagal: '+e.message); }
};

window.hapusTambahan = async (bagian, jenis) => {
  if(!confirm(`Hapus "${jenis}" dari ${bagian}?`)) return;
  showLoading('Menghapus...');
  try {
    const key = 'tambahan_item|' + bagian + '|' + jenis;
    if(_hargaDocIds[key]) { await sbDelete(sbDoc(db, COL.harga, _hargaDocIds[key])); delete _hargaDocIds[key]; }
    if(MASTER.tambahanItemList) { MASTER.tambahanItemList = MASTER.tambahanItemList.filter(t=>!(t.bagian===bagian&&t.jenis===jenis)); }
    showToast('🗑 Dihapus!');
    _renderTamDaftarBagian(bagian);
    renderDaftarTambahan();
  } catch(e){ showToast('❌ Gagal: '+e.message); }
  finally { hideLoading(); }
};

window.simpanTambahanBaru = async () => {
  const bagian = $('tam_bagian_sel')?.value;
  const jenis = $('tam_new_nama')?.value.trim().toUpperCase();
  const harga = parseInt($('tam_new_harga')?.value)||0;
  const satuan = $('tam_new_satuan')?.value || 'HARI';
  if(!bagian){ showToast('⚠ Pilih Bagian!'); return; }
  if(!jenis || !harga){ showToast('⚠ Isi nama & harga!'); return; }
  showLoading('Menyimpan...');
  try {
    const key = 'tambahan_item|' + bagian + '|' + jenis;
    if(_hargaDocIds[key]) { await sbUpdate(sbDoc(db, COL.harga, _hargaDocIds[key]), { harga, satuan }); const idx = (MASTER.tambahanItemList||[]).findIndex(t=>t.bagian===bagian&&t.jenis===jenis); if(idx>=0) { MASTER.tambahanItemList[idx].harga = harga; MASTER.tambahanItemList[idx].satuan = satuan; } }
    else { const ref = await sbInsert(sbTable(db, COL.harga), { type:'tambahan_item', bagian, jenis, harga, satuan, waktu:sbTimestamp() }); _hargaDocIds[key] = ref.id; if(!MASTER.tambahanItemList) MASTER.tambahanItemList = []; MASTER.tambahanItemList.push({ id:ref.id, bagian, jenis, harga, satuan }); }
    showToast('✅ Item ditambahkan!');
    $('tam_new_nama').value = ''; $('tam_new_harga').value = ''; if($('tam_new_satuan')) $('tam_new_satuan').value = 'HARI';
    const fb = $('tam_form_baru'); if(fb) fb.style.display = 'none';
    const btn = $('btnTamTambahItem'); if(btn) btn.textContent = '+ Tambah Item Lain';
    setDis('btnTamSimpanBaru', true);
    _renderTamDaftarBagian(bagian);
    renderDaftarTambahan();
  } catch(e){ showToast('❌ Gagal: '+e.message); }
  finally { hideLoading(); }
};

window.toggleDaftarTambahan = () => {
  const body = $('daftarTamBody');
  const chev = $('daftarTamChevron');
  if(body.style.display === 'none') { body.style.display = 'block'; chev.style.transform = 'rotate(180deg)'; renderDaftarTambahan(); }
  else { body.style.display = 'none'; chev.style.transform = ''; }
};

function renderDaftarTambahan() {
  const body = $('daftarTamBody');
  if(!body || body.style.display==='none') return;
  const list = (MASTER.tambahanItemList||[]);
  if(!list.length) { body.innerHTML = `<div style="text-align:center;color:var(--muted);font-size:12px;padding:12px;background:var(--card2);border-radius:12px;">Belum ada data tambahan</div>`; return; }
  const grouped = {};
  list.forEach(t => { const k = t.bagian||'-'; if(!grouped[k]) grouped[k] = []; grouped[k].push(t); });
  let html = '';
  Object.keys(grouped).sort().forEach(bag => {
    html += `<div style="margin-bottom:12px;"><div style="font-size:10px;font-weight:800;letter-spacing:.09em;text-transform:uppercase;color:var(--primary3);padding:6px 10px;background:var(--primaryDim);border-radius:8px;margin-bottom:6px;">${bag}</div>`;
    grouped[bag].sort((a,b)=>a.jenis.localeCompare(b.jenis)).forEach(t => {
      html += `<div style="background:var(--card2);border-radius:12px;padding:10px 14px;margin-bottom:6px;border:1px solid var(--muted);display:flex;justify-content:space-between;align-items:center;">
        <div style="font-size:13px;font-weight:700;color:var(--text);">${t.jenis||'-'}</div>
        <div style="font-size:13px;font-weight:800;color:var(--text);">Rp ${fmt(t.harga)} <span style="font-size:10px;font-weight:700;color:var(--muted);">/${(t.satuan||'HARI').toLowerCase()}</span></div>
      </div>`;
    });
    html += `</div>`;
  });
  body.innerHTML = html;
}

// ===== LIST =====
const listTitles = { pegawai:'Pegawai', bagian:'Bagian', subbagian:'Pekerjaan', brand:'Brand', jenisPinjaman:'Jenis Pinjaman' };
window.bukaList = type => {
  listType = type;
  $('listPopupTitle').textContent = listTitles[type]||type;
  $('listAddInput').value = '';
  const indukEl = $('listSubBagianInduk');
  if(type === 'subbagian') {
    indukEl.style.display = 'block';
    indukEl.innerHTML = '<option value="">-- Pilih Bagian Induk --</option>';
    MASTER.bagian.forEach(b => indukEl.innerHTML += `<option value="${b}">${b}</option>`);
    indukEl.value = '';
    $('listAddInput').placeholder = 'Nama pekerjaan baru...';
    $('listAddInput').disabled = true;
    $('listPopupBody').innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px 16px;font-size:13px;">Pilih Bagian Induk terlebih dahulu</div>';
  } else {
    indukEl.style.display = 'none';
    $('listAddInput').placeholder = 'Tambah baru...';
    $('listAddInput').disabled = false;
    renderListBody();
  }
  $('listPopup').classList.add('active');
};

window.onPilihBagianInduk = () => {
  const bagian = $('listSubBagianInduk').value;
  $('listAddInput').disabled = !bagian;
  $('listAddInput').value = '';
  renderListBody();
};

window.tutupList = () => $('listPopup').classList.remove('active');

function getListData() {
  if(listType==='pegawai') return MASTER.pegawai;
  if(listType==='bagian') return MASTER.bagian;
  if(listType==='brand') return MASTER.brands;
  if(listType==='jenisPinjaman') return MASTER.jenisPinjaman;
  if(listType==='subbagian') {
    const bagian = $('listSubBagianInduk').value;
    if(!bagian) return [];
    return MASTER.subBagian[bagian] || [];
  }
  return [];
}

function renderListBody() {
  const bodyEl = $('listPopupBody');
  if(listType === 'subbagian' && !$('listSubBagianInduk').value) {
    bodyEl.innerHTML = '<div style="color:var(--muted);text-align:center;padding:20px 16px;font-size:13px;">Pilih Bagian Induk terlebih dahulu</div>';
    return;
  }
  const items = getListData();
  if(!items.length) { 
    bodyEl.innerHTML = `<div class="empty-state-box" style="padding:16px;"><i class="fa-solid fa-box-open"></i><div class="empty-state-box-text">Belum ada data</div></div>`; 
    return; 
  }
  bodyEl.innerHTML = items.map((item,i) => `<div class="list-item"><span class="list-item-name">${item}</span><button class="list-item-del" onclick="hapusListItem(${i})">Hapus</button></div>`).join('');
}

window.hapusListItem = async idx => {
  const value=getListData()[idx];
  if(!value) return;
  showLoading('Menghapus...');
  try {
    const docId=_masterDocIds[listType+'|'+value];
    if(docId) await sbDelete(sbDoc(db,COL.master,docId));
    showToast('✅ Dihapus');
  } catch(e){ showToast('❌ Gagal'); }
  finally { hideLoading(); }
};

window.tambahListItem = async () => {
  const val = $('listAddInput').value.trim().toUpperCase();
  if(!val){ showToast('⚠ Isi nama terlebih dahulu!'); return; }
  showLoading('Menyimpan...');
  try {
    if(listType==='subbagian') {
      const bi=$('listSubBagianInduk').value;
      if(!bi){ hideLoading(); showToast('⚠ Pilih bagian induk!'); return; }
      await sbInsert(sbTable(db,COL.master), { type:'subbagian', nama:val, bagianInduk:bi, waktu:sbTimestamp() });
    } else {
      if(getListData().includes(val)){ hideLoading(); showToast('⚠ Data sudah ada!'); return; }
      await sbInsert(sbTable(db,COL.master), { type:listType, nama:val, waktu:sbTimestamp() });
    }
    $('listAddInput').value='';
    showToast('✅ Ditambahkan');
  } catch(e){ showToast('❌ Gagal'); }
  finally { hideLoading(); }
};

// ===== REKAP =====
window.openRekap = () => {
  $('rekapPage').classList.add('active');
  history.pushState({ page: 'rekapPage' }, '');

  // Sync toggle UI ke state aktual (_weekToggleOn)
  const track = document.getElementById('weekToggleTrack');
  const thumb = document.getElementById('weekToggleThumb');
  const label = document.getElementById('weekToggleLabel');
  const wrap  = document.getElementById('weekToggleWrap');
  if (_weekToggleOn) {
    track.style.background = 'var(--primary)';
    thumb.style.left = '18px';
    label.style.color = 'var(--primary)';
    label.textContent = 'MINGGU INI';
    wrap.style.borderColor = 'var(--primary)';
  } else {
    track.style.background = 'var(--muted)';
    thumb.style.left = '2px';
    label.style.color = 'var(--muted)';
    label.textContent = 'MINGGU LALU';
    wrap.style.borderColor = 'var(--muted)';
  }

  gantiTab('pegawai');
  muatRekapPinjaman();
};
window.tutupRekap = () => { $('rekapPage').classList.remove('active'); if (history.state && history.state.page === 'rekapPage') history.back(); _dashboardStale = false; loadDashboardDana(); };

window.gantiTab = tab => {
  currentTab = tab;
  batalModePilih(); // reset ke mode normal setiap ganti tab, biar Cetak/Thermal ga nyangkut
  $('tabPegawai').classList.toggle('active', tab==='pegawai');
  $('tabPinjaman').classList.toggle('active', tab==='pinjaman');
  $('tabPegawai').style.background = tab==='pegawai' ? 'linear-gradient(135deg, var(--primary3), var(--primary))' : 'transparent';
  $('tabPegawai').style.color = tab==='pegawai' ? '#fff' : 'var(--muted)';
  $('tabPegawai').style.boxShadow = tab==='pegawai' ? '0 2px 8px rgba(26,86,219,.25)' : 'none';
  $('tabPegawai').style.fontWeight = tab==='pegawai' ? '800' : '700';
  $('tabPinjaman').style.background = tab==='pinjaman' ? 'linear-gradient(135deg, var(--primary3), var(--primary))' : 'transparent';
  $('tabPinjaman').style.color = tab==='pinjaman' ? '#fff' : 'var(--muted)';
  $('tabPinjaman').style.boxShadow = tab==='pinjaman' ? '0 2px 8px rgba(26,86,219,.25)' : 'none';
  $('tabPinjaman').style.fontWeight = tab==='pinjaman' ? '800' : '700';
  $('tablePegawai').style.display = 'none';
  $('tablePinjaman').style.display = tab==='pinjaman' ? '' : 'none';
  $('rekapCardList').style.display = tab==='pegawai' ? 'flex' : 'none';
  $('rekapTitle').textContent = tab==='pegawai' ? 'Rekap Pegawai' : 'Rekap Pinjaman';
  $('rekapDateBox').style.display = tab==='pegawai' ? '' : 'none';
  const legendaEl = $('legendaPinjaman');
  if(legendaEl) legendaEl.style.display = tab==='pegawai' ? 'flex' : 'none';
  if(tab==='pegawai') muatRekapPegawai();
  else { muatRekapPinjaman(); }
};

function muatRekapPegawai(filter='') {
  const dari=_getISO('rekapMulai'), sampai=_getISO('rekapAkhir');
  const items = getInputDataLocal(dari, sampai);
  const grouped = {};
  items.forEach(r => {
    if(!MASTER.pegawai.includes(r.nama)) return;
    if(!grouped[r.nama]) grouped[r.nama]={total:0};
    grouped[r.nama].total += (r.jumlah||0);
  });
  const potMap = {};
  _CACHE.bayar.filter(b => { if(dari && b.tanggalISO<dari) return false; if(sampai && b.tanggalISO>sampai) return false; if(b.luarGaji) return false; return true; }).forEach(b => { potMap[b.nama]=(potMap[b.nama]||0)+(b.bayar||0); });
  rekapRows = Object.entries(grouped).map(([nama,v]) => { const pot=potMap[nama]||0; return { nama, total:v.total, potongan:pot, bersih:v.total-pot }; }).sort((a,b)=>a.nama.localeCompare(b.nama));
  renderTabelRekap(filter);
}

function renderTabelRekap(filter='') {
  const container = $('rekapCardList');
  const rows = filter ? rekapRows.filter(r => r.nama.toLowerCase().includes(filter.toLowerCase())) : rekapRows;
  
  if (!rows.length) {
    container.innerHTML = `
      <div class="empty-state-box">
        <i class="fa-solid fa-inbox"></i>
        <div class="empty-state-box-text">Belum ada data untuk periode ini</div>
      </div>
    `;
    return;
  }

  const _dari = _getISO('rekapMulai');
  const _sampai = _getISO('rekapAkhir');
  
  // Build pinjaman map
  const pinjamanAktifMap = {};
  _CACHE.pinjaman.forEach(p => {
    if (!pinjamanAktifMap[p.nama]) pinjamanAktifMap[p.nama] = { total: 0, jenis: new Set() };
    pinjamanAktifMap[p.nama].total += (p.nominal || 0);
    if (p.jenis) pinjamanAktifMap[p.nama].jenis.add(p.jenis);
  });
  _CACHE.bayar.forEach(b => { 
    if (pinjamanAktifMap[b.nama]) { 
      pinjamanAktifMap[b.nama].total -= (b.bayar || 0); 
    } 
  });

  // Summary bar
  let totalKotor = 0, totalTambahan = 0, totalPotongan = 0, totalBersih = 0;
  rows.forEach(r => {
    const tam = _rcTambahan[r.nama];
    const tamTgl = tam?._tanggalISO || '';
    // Tanpa tanggal = tidak valid, harus dianggap di luar range (bukan otomatis lolos)
    const tamDalamRange = tamTgl && ((!_dari || tamTgl >= _dari) && (!_sampai || tamTgl <= _sampai));
    const tamTotalFromItems = (tam && tam.items) ? Object.values(tam.items).reduce((s, it) => s + (it.jumlah > 0 ? it.jumlah : (it.qty||0)*(it.harga||0)), 0) : 0;
    const tamTotal = tamDalamRange ? ((tam && tam.total > 0) ? tam.total : tamTotalFromItems) : 0;
    const pinData = pinjamanAktifMap[r.nama];
    const sisaPinjaman = pinData ? pinData.total : 0;
    const adaPinjaman = sisaPinjaman > 0;
    const bersihFinal = r.total + tamTotal - r.potongan;
    
    totalKotor += r.total;
    totalTambahan += tamTotal;
    totalPotongan += r.potongan;
    totalBersih += bersihFinal;
  });

  let html = ``;

  // Card list
  rows.forEach((r, idx) => {
    const tam = _rcTambahan[r.nama];
    const tamTgl = tam?._tanggalISO || '';
    const tamDalamRange = !tamTgl || ((!_dari || tamTgl >= _dari) && (!_sampai || tamTgl <= _sampai));
    const tamTotalFromItems = (tam && tam.items) ? Object.values(tam.items).reduce((s, it) => s + (it.jumlah > 0 ? it.jumlah : (it.qty||0)*(it.harga||0)), 0) : 0;
    const tamTotal = tamDalamRange ? ((tam && tam.total > 0) ? tam.total : tamTotalFromItems) : 0;
    const adaTambahan = tamTotal > 0;
    const pinData = pinjamanAktifMap[r.nama];
    const sisaPinjaman = pinData ? pinData.total : 0;
    const adaPinjaman = sisaPinjaman > 0;
    const bersihFinal = r.total + tamTotal - r.potongan;
    
    const q0 = s => String(s).replace(/'/g, "\\'");

    // Rincian item: Pekerjaan
    const _dataRincian = getRincianLocal(r.nama, _dari, _sampai);
    window._rekapPekerjaanRaw = window._rekapPekerjaanRaw || {};
    window._rekapPekerjaanRaw[idx] = _dataRincian.items;
    let pekerjaanHTML = '';
    if (_dataRincian.items.length > 0) {
      const _merged = {};
      _dataRincian.items.forEach(it => {
        const key = (it.jenis || '-') + '||' + (it.harga || 0);
        if (!_merged[key]) _merged[key] = { jenis: it.jenis, harga: it.harga, qty: 0, jumlah: 0 };
        _merged[key].qty += (it.qty || 0);
        _merged[key].jumlah += (it.jumlah || 0);
      });
      pekerjaanHTML = Object.values(_merged).map(it =>
        `<div class="rekap-breakdown-row rekap-breakdown-clickable" onclick="event.stopPropagation(); bukaDetailPekerjaan(${idx}, '${q0(it.jenis || '-')}', ${it.harga || 0})">
          <span>${it.jenis || '-'} <small>(${it.qty || 0} x Rp${fmt(it.harga)})</small></span>
          <span style="display:flex;align-items:center;">Rp ${fmt(it.jumlah)} <i class="fa-solid fa-chevron-right" style="font-size:12px;color:var(--muted);margin-left:8px;width:22px;height:22px;display:flex;align-items:center;justify-content:center;flex-shrink:0;"></i></span>
        </div>`
      ).join('');
    }
    
    // Rincian item: Tambahan
    let tambahanHTML = '';
    if (tam && tam.items) {
      tambahanHTML = Object.values(tam.items).filter(it => (it.jumlah > 0) || (it.qty > 0 && it.harga > 0)).map(it => {
        const jumlahTampil = it.jumlah > 0 ? it.jumlah : (it.qty || 0) * (it.harga || 0);
        return `<div class="rekap-breakdown-row"><span>${it.jenis} <small>(${it.qty} ${(it.satuan||'HARI').toLowerCase()})</small></span><span class="positive">+Rp ${fmt(jumlahTampil)}</span></div>`;
      }).join('');
    }
    
    // Rincian item: Potongan
    const _bayarItemsRow = _CACHE.bayar.filter(b => b.nama===r.nama && !b.luarGaji && (!_dari||b.tanggalISO>=_dari) && (!_sampai||b.tanggalISO<=_sampai));
    const _potMapRow = {};
    _bayarItemsRow.forEach(b => { const j=b.jenisPinjaman||'Pinjaman'; _potMapRow[j]=(_potMapRow[j]||0)+(b.bayar||0); });
    const potonganHTML = Object.entries(_potMapRow).map(([jenis,jumlah]) =>
      `<div class="rekap-breakdown-row"><span>${jenis}</span><span class="negative">-Rp ${fmt(jumlah)}</span></div>`
    ).join('');
    const bagian = (MASTER.pegawaiData[r.nama]?.bagian || '-');
    const initials = r.nama.split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase();
    
    // Format amount
    // Format amount: selalu Rupiah penuh
    const amountDisplay = fmt(bersihFinal);
    
    const q = s => String(s).replace(/'/g, "\\'");
    
    html += `
      <div class="rekap-card ${adaPinjaman ? 'has-pinjaman' : 'no-pinjaman'}" id="rekapCard_${idx}" onclick="toggleCardExpand(${idx})">
        
        <div class="rekap-card-header">
          <input type="checkbox" class="rekap-select row-check-rekap" value="${q(r.nama)}" onclick="event.stopPropagation(); onCardSelect(this, '${q(r.nama)}')" style="margin-right:4px;">
          <div class="rekap-avatar ${adaPinjaman ? 'pinjaman' : ''}">${initials}</div>
          <div class="rekap-info">
            <div class="rekap-name-row">
              <span class="rekap-name">${r.nama}</span>
              ${adaTambahan ? `<span class="rekap-badge tambahan"><i class="fa-solid fa-plus" style="font-size:7px;"></i> Tambahan</span>` : ''}
              ${adaPinjaman ? `<span class="rekap-badge pinjaman"><i class="fa-solid fa-triangle-exclamation" style="font-size:7px;"></i> Pinjaman</span>` : `<span class="rekap-badge lunas"><i class="fa-solid fa-check" style="font-size:7px;"></i> Lunas</span>`}
            </div>
            <div class="rekap-bagian">${bagian}</div>
          </div>
        </div>
        <div style="display:flex; align-items:center; padding:0 16px 14px; gap:0;">
          <div class="rekap-amount-main" style="flex:1;">
            <div class="rekap-amount-row" style="align-items:flex-start; text-align:left; padding-left:0;">
              <div class="rekap-amount-label">Pendapatan Kotor</div>
              <div class="rekap-amount-value">Rp ${fmt(r.total + tamTotal)}</div>
            </div>
            <div class="rekap-amount-row">
              <div class="rekap-amount-label">Potongan</div>
              <div class="rekap-amount-value red">${r.potongan > 0 ? '-Rp ' + fmt(r.potongan) : '-'}</div>
            </div>
            <div class="rekap-amount-row" style="align-items:flex-end; text-align:right; padding-right:0;">
              <div class="rekap-amount-label">Diterima</div>
              <div class="rekap-amount-value bersih ${bersihFinal < 0 ? 'warning' : ''}">Rp ${amountDisplay}</div>
            </div>
          </div>
          <div class="rekap-expand-hint" style="justify-content:flex-end; padding:0; margin-left:12px; flex-shrink:0;">
            <span>Detail</span>
            <i class="fa-solid fa-chevron-down"></i>
          </div>
        </div>
        
        <div class="rekap-card-body" id="rekapCardBody_${idx}">
          <div class="rekap-breakdown">
            ${pekerjaanHTML ? `
            <div class="rekap-breakdown-section">
              <div class="rekap-breakdown-title">Pekerjaan</div>
              ${pekerjaanHTML}
              <div class="rekap-breakdown-row total"><span>Subtotal</span><span>Rp ${fmt(_dataRincian.subtotal)}</span></div>
            </div>` : ''}
            ${tambahanHTML ? `
            <div class="rekap-breakdown-section tambahan">
              <div class="rekap-breakdown-title">Tambahan</div>
              ${tambahanHTML}
              <div class="rekap-breakdown-row total"><span>Total</span><span class="positive">+Rp ${fmt(tamTotal)}</span></div>
            </div>` : ''}
            ${potonganHTML ? `
            <div class="rekap-breakdown-section potongan">
              <div class="rekap-breakdown-title">Potongan</div>
              ${potonganHTML}
              <div class="rekap-breakdown-row total"><span>Total</span><span class="negative">-Rp ${fmt(r.potongan)}</span></div>
            </div>` : ''}
          </div>
          <div class="rekap-detail-grid" style="grid-template-columns:1fr;">
            <div class="rekap-detail-item">
              <div class="rekap-detail-label">Pinjaman Sisa</div>
              <div class="rekap-detail-value ${adaPinjaman ? 'negative' : 'positive'}">${adaPinjaman ? 'Rp ' + fmt(sisaPinjaman) : '✓ Lunas'}</div>
            </div>
          </div>
          
          <div class="rekap-actions">
            <button class="rekap-action-btn" onclick="event.stopPropagation(); bukaTambahanPopup('${q(r.nama)}')">
              <i class="fa-solid fa-circle-plus"></i> Tambahan
            </button>
            ${adaPinjaman ? `<button class="rekap-action-btn danger" onclick="event.stopPropagation(); bukaBayarForNama('${q(r.nama)}')">
              <i class="fa-solid fa-money-bill-transfer"></i> Bayar
            </button>` : `<button class="rekap-action-btn" onclick="event.stopPropagation(); bukaPinjamanForNama('${q(r.nama)}')">
              <i class="fa-solid fa-hand-holding-dollar"></i> Pinjam
            </button>`}
            <button class="rekap-action-btn primary" onclick="event.stopPropagation(); bukaSlipGaji('${q(r.nama)}')">
              <i class="fa-solid fa-print"></i> Cetak
            </button>
          </div>
        </div>
      </div>
    `;
  });
  
  container.innerHTML = html;
  
  // Sync checkbox state
  const checkAll = $('checkAllRekap');
  if (checkAll) checkAll.checked = false;
}

// ===== TAMBAHKAN FUNGSI BARU =====

// ===== DETAIL PEKERJAAN (koreksi & hapus per-entri, dengan lock periode) =====
function _isPeriodeLewat(tanggalISO) {
  if (!tanggalISO) return true; // data tanpa tanggal valid, amankan: kunci
  const seninIni = _getMondayISO(new Date());
  return tanggalISO < seninIni;
}

window.bukaDetailPekerjaan = (idx, jenis, harga) => {
  window._lastDetailCtx = { idx, jenis, harga };
  const raw = (window._rekapPekerjaanRaw && window._rekapPekerjaanRaw[idx]) || [];
  const list = raw
    .filter(it => (it.jenis || '-') === jenis && (it.harga || 0) === harga)
    .sort((a, b) => (b.tanggalISO || '').localeCompare(a.tanggalISO || ''));
  if (!list.length) { showToast('Data tidak ditemukan'); return; }

  const rowsHtml = list.map(it => {
    const lewat = _isPeriodeLewat(it.tanggalISO);
    const tglLabel = it.tanggalISO ? it.tanggalISO.split('-').reverse().join('/') : '-';
    return `<div style="background:var(--card2);border-radius:12px;padding:10px 12px;margin-bottom:8px;border:1px solid var(--muted);">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
        <div style="font-size:11px;font-weight:700;color:var(--muted);"><i class="fa-solid fa-calendar-day" style="margin-right:4px;"></i>${tglLabel}</div>
        <div style="font-size:13px;font-weight:800;color:var(--text);">${it.qty || 0} x Rp${fmt(harga)} = Rp ${fmt(it.jumlah || 0)}</div>
      </div>
      <div style="display:flex;gap:8px;">
        <button ${lewat ? 'disabled' : ''} onclick="window.tutupDetailPekerjaan(); bukaEditQty('${it.id}', ${it.qty || 0}, ${harga})"
          style="flex:1;padding:9px;border-radius:10px;border:1.5px solid ${lewat ? 'var(--muted)' : 'var(--primary)'};background:${lewat ? 'var(--bg2)' : 'var(--primaryDim)'};color:${lewat ? 'var(--muted)' : 'var(--primary3)'};font-size:12px;font-weight:700;font-family:inherit;cursor:${lewat ? 'not-allowed' : 'pointer'};">
          <i class="fa-solid fa-pencil"></i> Koreksi
        </button>
        <button ${lewat ? 'disabled' : ''} onclick="window.hapusPekerjaanItem('${it.id}')"
          style="flex:1;padding:9px;border-radius:10px;border:1.5px solid ${lewat ? 'var(--muted)' : 'var(--red)'};background:${lewat ? 'var(--bg2)' : '#FEE2E2'};color:${lewat ? 'var(--muted)' : 'var(--red)'};font-size:12px;font-weight:700;font-family:inherit;cursor:${lewat ? 'not-allowed' : 'pointer'};">
          <i class="fa-solid fa-trash"></i> Hapus
        </button>
      </div>
      ${lewat ? '<div style="font-size:10px;color:var(--muted);margin-top:7px;text-align:center;"><i class="fa-solid fa-lock"></i>&nbsp;Periode minggu ini sudah lewat, tidak bisa diubah</div>' : ''}
    </div>`;
  }).join('');

  const overlay = document.createElement('div');
  overlay.id = 'detailPekerjaanOverlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:99999999;display:flex;align-items:flex-end;justify-content:center;padding:16px;';
  overlay.innerHTML = `<div style="background:var(--card);border-radius:20px;padding:20px;width:100%;max-width:420px;max-height:75vh;overflow-y:auto;">
    <div style="font-size:14px;font-weight:800;color:var(--text);margin-bottom:2px;">${jenis}</div>
    <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.06em;text-transform:uppercase;margin-bottom:14px;">Rincian per tanggal</div>
    ${rowsHtml}
    <button onclick="window.tutupDetailPekerjaan()" style="width:100%;background:transparent;color:var(--muted);border:1.5px solid var(--muted);border-radius:12px;padding:12px;font-size:13px;font-weight:700;cursor:pointer;font-family:inherit;margin-top:4px;">Tutup</button>
  </div>`;
  document.body.appendChild(overlay);
};

window.tutupDetailPekerjaan = () => {
  const el = document.getElementById('detailPekerjaanOverlay');
  if (el) el.remove();
};

window.hapusPekerjaanItem = (id) => {
  bukaConfirm({
    icon: '<i class="fa-solid fa-trash"></i>',
    iconBg: '#FEE2E2', iconColor: '#EF4444',
    title: 'Hapus Entri Pekerjaan?',
    msg: 'Entri ini akan dihapus permanen dan tidak bisa dikembalikan.',
    okLabel: 'Ya, Hapus',
    okBg: 'linear-gradient(135deg,#DC2626,#EF4444)',
    callback: async () => {
      showLoading('Menghapus...');
      try {
        await sbDelete(sbDoc(db, COL.input, id));
        _CACHE.input = _CACHE.input.filter(r => r.id !== id);
        window.tutupDetailPekerjaan();
        showToast('🗑 Entri berhasil dihapus');
        if (currentTab === 'pegawai') muatRekapPegawai();
      } catch (e) { showToast('❌ Gagal hapus: ' + e.message); }
      finally { hideLoading(); }
    }
  });
};

window.toggleCardExpand = function(idx) {
  const card = document.getElementById('rekapCard_' + idx);
  const body = document.getElementById('rekapCardBody_' + idx);
  if (!card || !body) return;
  
  const isOpen = body.classList.contains('open');
  
  // Close all other cards (accordion style - optional, remove if want multiple open)
  document.querySelectorAll('.rekap-card-body.open').forEach(el => {
    if (el.id !== 'rekapCardBody_' + idx) {
      el.classList.remove('open');
      el.closest('.rekap-card').classList.remove('open');
    }
  });
  
  if (isOpen) {
    body.classList.remove('open');
    card.classList.remove('open');
  } else {
    body.classList.add('open');
    card.classList.add('open');
    // Scroll card ke atas agar terlihat dari awal
    setTimeout(() => {
      card.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 50);
  }
};

window.onCardSelect = function(checkbox, nama) {
  updateSelectAllUI();
};

function updateSelectAllUI() {
  const allChecked = document.querySelectorAll('.row-check-rekap:checked').length;
  const total = document.querySelectorAll('.row-check-rekap').length;
  ['checkAllRekap', 'checkAllRekapCard'].forEach(id => {
    const el = $(id);
    if (el) {
      el.checked = allChecked === total && total > 0;
      el.indeterminate = allChecked > 0 && allChecked < total;
    }
  });
  // Update label cetak
  const label = $('cetakMassalLabel');
  if (label) label.textContent = allChecked > 0 ? `Cetak (${allChecked})` : 'Cetak';
  // Update counter di bar Pilih Semua
  const countEl = $('selectAllCount');
  if (countEl) countEl.textContent = allChecked > 0 ? `${allChecked} dipilih` : '';
}

window.masukModePilih = function() {
  // Uncheck semua dulu
  document.querySelectorAll('.row-check-rekap').forEach(el => { el.checked = false; });
  // Aktifkan mode pilih
  $('rekapCardList').classList.add('mode-pilih');
  // Toggle tombol
  $('btnModePilih').style.display = 'none';
  $('btnBatalPilih').style.display = 'flex';
  $('btnCetakMassal').style.display = 'flex';
  const _btnTM2 = $('btnThermalMassal'); if(_btnTM2) _btnTM2.style.display = 'flex';
  const label = $('cetakMassalLabel');
  if (label) label.textContent = 'Cetak';
  // Tampilkan bar Pilih Semua
  const bar = $('selectAllBar'); if (bar) bar.style.display = 'flex';
  const cAll = $('checkAllRekapCard'); if (cAll) { cAll.checked = false; cAll.indeterminate = false; }
  const countEl = $('selectAllCount'); if (countEl) countEl.textContent = '';
};

window.batalModePilih = function() {
  document.querySelectorAll('.row-check-rekap').forEach(el => { el.checked = false; });
  $('rekapCardList').classList.remove('mode-pilih');
  $('btnModePilih').style.display = 'flex';
  $('btnBatalPilih').style.display = 'none';
  $('btnCetakMassal').style.display = 'none';
  const _btnTM3 = $('btnThermalMassal'); if(_btnTM3) _btnTM3.style.display = 'none';
  // Sembunyikan bar Pilih Semua
  const bar = $('selectAllBar'); if (bar) bar.style.display = 'none';
};

window.bukaBayarForNama = function(nama) {
  // Buka popup dulu (akan reset form & isi dropdown)
  bukaBayar();
  // Setelah popup dibuka & dropdown diisi, baru pre-select nama
  requestAnimationFrame(() => {
    $('bayar_nama').value = nama;
    onBayarNama();
  });
};

window.bukaPinjamanForNama = function(nama) {
  bukaPinjaman();
  // Pre-select nama di popup pinjaman (dipanggil SETELAH bukaPinjaman
  // supaya tidak ditimpa ulang oleh reset default-nya)
  $('pin_nama').value = nama;
  onPinNama();
};

window.toggleCheckAllRekap = function(cb) {
  document.querySelectorAll('.row-check-rekap').forEach(el => el.checked = cb.checked);
  // Sinkronkan pasangan checkbox "pilih semua" lainnya (tabel & card pakai id berbeda)
  const other = $(cb.id === 'checkAllRekap' ? 'checkAllRekapCard' : 'checkAllRekap');
  if (other) other.checked = cb.checked;
  updateSelectAllUI();
};

function muatRekapPinjaman() {
  rekapPinRows = getRekapPinjamanLocal().filter(r => r.sisa > 0);
  renderTabelPinjaman();
}

function renderTabelPinjaman(filter='') {
  const tbody = $('tbodyPinjaman');
  const rows = filter ? rekapPinRows.filter(r => r.nama.toLowerCase().includes(filter.toLowerCase())) : rekapPinRows;
  
  if(!rows.length) {
    tbody.innerHTML = `<tr><td colspan="6"><div class="empty-state-box"><i class="fa-solid fa-check-circle"></i><div class="empty-state-box-text">Tidak ada pinjaman aktif</div></div></td></tr>`;
    return;
  }
  
  const q = s => String(s).replace(/'/g,"\\'");
  
  tbody.innerHTML = rows.map(r => {
    return `<tr>
      <td class="nama-cell" style="font-weight:700;font-size:11px;word-break:break-word;">${r.nama}</td>
      <td style="font-size:10px;font-weight:700;color:var(--primary3);word-break:break-word;line-height:1.3;">${r.jenis && r.jenis !== '-' ? (() => { const arr = r.jenis.split(', '); return arr.length === 1 ? arr[0] : `<span style="display:inline-block;background:var(--primaryDim);border-radius:6px;padding:2px 6px;">${arr.length} jenis</span>`; })() : '-'}</td>
      <td style="font-weight:600;font-size:11px;">${fmt(r.pinjaman)}</td>
      <td style="color:var(--green);font-weight:600;font-size:11px;">${r.bayar > 0 ? fmt(r.bayar) : '-'}</td>
      <td style="color:var(--red);font-weight:700;font-size:11px;">${fmt(r.sisa)}</td>
      <td style="padding:5px 3px;">
        <div style="display:flex;flex-direction:row;gap:4px;justify-content:center;align-items:center;flex-wrap:nowrap;">
          <button onclick="bukaEditBayar('${q(r.nama)}')" 
            style="flex:1;min-width:0;background:var(--primaryDim);border:none;border-radius:8px;padding:7px 4px;cursor:pointer;color:var(--primary3);font-size:10px;font-weight:800;white-space:nowrap;display:flex;align-items:center;justify-content:center;gap:3px;">
            <i class="fa-solid fa-pen-to-square"></i> Edit
          </button>
        </div>
      </td>
    </tr>`;
  }).join('');
}

// ===== EDIT BAYAR — POPUP LIST =====

let _editBayarCurrentNama = '';
let _editBayarCurrentId = '';

window.bukaEditBayar = (nama) => {
  const bayarList = _CACHE.bayar.filter(b => b.nama === nama && !b.luarGaji)
    .sort((a, b) => b.tanggalISO.localeCompare(a.tanggalISO));
  
  _editBayarCurrentNama = nama;
  
  $('editBayarNama').textContent = nama;
  
  const body = $('editBayarBody');
  
  // Hitung sisa pinjaman saat ini
  const pinjamanData = getRekapPinjamanPerJenisLocal(nama);
  const totalSisa = pinjamanData.reduce((sum, p) => sum + p.sisa, 0);
  
  let html = '';
  
  // Info sisa
  html += `<div class="edit-bayar-sisa-info">
    <span class="edit-bayar-sisa-label">Sisa Pinjaman Saat Ini</span>
    <span class="edit-bayar-sisa-val">Rp ${fmt(totalSisa)}</span>
  </div>`;
  
  if (!bayarList.length) {
    // Belum ada pembayaran — tampilkan empty state
    html += `<div class="empty-state-box">
      <i class="fa-solid fa-receipt"></i>
      <div class="empty-state-box-text">Belum ada pembayaran untuk diedit</div>
    </div>`;
  } else {
    // List pembayaran — HANYA Edit Nominal
    html += bayarList.map((b, i) => `
      <div class="edit-bayar-item">
        <div class="edit-bayar-header">
          <div>
            <div class="edit-bayar-jenis">${b.jenisPinjaman || 'Pinjaman'}</div>
            <div class="edit-bayar-tanggal">${b.tanggal || '-'} • ${b.tanggalISO || ''}</div>
          </div>
          <div class="edit-bayar-nominal">Rp ${fmt(b.bayar || 0)}</div>
        </div>
        <div class="edit-bayar-actions">
          <button onclick="bukaEditNominalBayar('${b.id}', ${b.bayar || 0}, ${totalSisa + (b.bayar || 0)})" 
            class="edit-bayar-btn edit-bayar-btn-edit" style="grid-column: 1 / -1;">
            <i class="fa-solid fa-pen"></i> Edit Nominal
          </button>
        </div>
      </div>
    `).join('');
  }
  
  // Zona berbahaya — hapus pinjaman
  html += `<div style="margin-top:18px;padding-top:14px;border-top:1.5px dashed var(--muted);">
    <button onclick="tutupPopup('editBayarPopup'); setTimeout(() => hapusPinjamanRekap('${nama.replace(/'/g,"\\'")}'), 200);" 
      class="edit-bayar-btn edit-bayar-btn-hapus" style="width:100%;">
      <i class="fa-solid fa-trash"></i> Hapus Pinjaman
    </button>
  </div>`;
  
  body.innerHTML = html;
  
  // Show popup
  const popup = $('editBayarPopup');
  const sheet = popup.querySelector('.popup-sheet');
  if (sheet) sheet.style.transform = 'translateY(100%)';
  popup.style.display = 'flex';
  if (sheet) {
    sheet.offsetHeight;
    requestAnimationFrame(() => { sheet.style.transform = 'translateY(0)'; });
  }
  
  _initKeyboardHandler();
};

window.hapusSemuaPinjaman = async (nama) => {
  // Get detail pinjaman untuk confirm
  const pinjamanData = getRekapPinjamanPerJenisLocal(nama);
  const totalPinjaman = pinjamanData.reduce((s, p) => s + p.pinjaman, 0);
  const totalBayar = pinjamanData.reduce((s, p) => s + p.bayar, 0);
  const totalSisa = pinjamanData.reduce((s, p) => s + p.sisa, 0);
  
  bukaConfirm({
    icon: '<i class="fa-solid fa-triangle-exclamation"></i>',
    iconBg: '#FEE2E2', 
    iconColor: '#EF4444',
    title: 'Hapus Semua Pinjaman?',
    msg: `<div style="text-align: left; line-height: 1.6;">
      <b>${nama}</b> akan dihapus dari data pinjaman:<br><br>
      ${pinjamanData.map(p => `• ${p.jenis}: <b>Rp ${fmt(p.pinjaman)}</b>`).join('<br>')}<br><br>
      <div style="display: flex; justify-content: space-between; padding: 8px 0; border-top: 1px solid #eee;">
        <span>Total Pinjaman:</span>
        <span style="color: #EF4444; font-weight: 800;">Rp ${fmt(totalPinjaman)}</span>
      </div>
      <div style="display: flex; justify-content: space-between; padding: 4px 0;">
        <span>Sudah Dibayar:</span>
        <span style="color: #10B981; font-weight: 800;">Rp ${fmt(totalBayar)}</span>
      </div>
      <div style="display: flex; justify-content: space-between; padding: 4px 0; border-top: 1px solid #eee;">
        <span>Sisa:</span>
        <span style="color: #EF4444; font-weight: 800;">Rp ${fmt(totalSisa)}</span>
      </div>
      <br><span style="color: #EF4444; font-size: 12px;">⚠ Tindakan ini tidak bisa dibatalkan!</span>
    </div>`,
    okLabel: 'Ya, Hapus Semua',
    okBg: 'linear-gradient(135deg,#DC2626,#EF4444)',
    callback: async () => {
      showLoading('Menghapus pinjaman...');
      try {
        // Hapus semua pinjaman
        const pinjamanDocs = _CACHE.pinjaman.filter(p => p.nama === nama);
        for (const p of pinjamanDocs) {
          await sbDelete(sbDoc(db, COL.pinjaman, p.id));
        }
        
        // Hapus semua bayar
        const bayarDocs = _CACHE.bayar.filter(b => b.nama === nama);
        for (const b of bayarDocs) {
          await sbDelete(sbDoc(db, COL.bayar, b.id));
        }
        
        showToast('🗑 Pinjaman & pembayaran ' + nama + ' dihapus');
        tutupPopup('editBayarPopup');
        setTimeout(() => {
          muatRekapPinjaman();
          muatRekapPegawai();
        }, 300);
      } catch (e) {
        showToast('❌ Gagal: ' + e.message);
      } finally {
        hideLoading();
      }
    }
  });
};

// ===== EDIT NOMINAL BAYAR — INLINE POPUP =====

window.bukaEditNominalBayar = (id, currentBayar, sisaSebelumBayar) => {
  _editBayarCurrentId = id;
  
  const sisa = sisaSebelumBayar - currentBayar;
  
  $('editNominalSubtitle').textContent = 'Ubah jumlah pembayaran';
  $('editNominalSisa').textContent = fmt(sisa + currentBayar); // Sisa sebelum bayar ini
  $('editNominalInput').value = currentBayar;
  $('editNominalDisplay').value = currentBayar > 0 ? 'Rp ' + fmt(currentBayar) : '';
  $('editNominalHint').innerHTML = `
    Nominal saat ini: <b>Rp ${fmt(currentBayar)}</b><br>
    <span style="color: var(--muted);">Input 0 untuk membatalkan pembayaran</span>
  `;
  
  // Show popup
  const popup = $('editNominalBayarPopup');
  const sheet = popup.querySelector('.popup-sheet');
  if (sheet) sheet.style.transform = 'translateY(100%)';
  popup.style.display = 'flex';
  if (sheet) {
    sheet.offsetHeight;
    requestAnimationFrame(() => { sheet.style.transform = 'translateY(0)'; });
  }
};

window.simpanEditNominalBayar = async () => {
  const newNominal = parseInt($('editNominalInput').value);
  
  // Bisa 0 untuk hapus pembayaran
  if (isNaN(newNominal) || newNominal < 0) {
    showToast('⚠ Nominal tidak valid');
    return;
  }
  
  if (!_editBayarCurrentId) {
    showToast('❌ ID tidak valid');
    return;
  }
  
  const btn = $('btnSimpanEditNominal');
  btn.innerHTML = '<i class="fa-solid fa-circle-notch fa-spin"></i><span>Menyimpan...</span>';
  btn.disabled = true;
  
  showLoading('Menyimpan perubahan...');
  
  try {
    if (newNominal === 0) {
      // Kalau 0, hapus pembayaran
      await sbDelete(sbDoc(db, COL.bayar, _editBayarCurrentId));
      showToast('🗑 Pembayaran dibatalkan (nominal di 0 kan)');
    } else {
      // Kalau > 0, update nominal
      await sbUpdate(sbDoc(db, COL.bayar, _editBayarCurrentId), { 
        bayar: newNominal,
        updatedAt: sbTimestamp()
      });
      showToast('✅ Pembayaran berhasil diupdate');
    }
    
    tutupPopup('editNominalBayarPopup');
    
    // Refresh list
    setTimeout(() => {
      // Cek masih ada bayar?
      const sisaBayar = _CACHE.bayar.filter(b => b.nama === _editBayarCurrentNama && !b.luarGaji);
      if (sisaBayar.length === 0 && newNominal === 0) {
        // Kalau semua bayar dihapus, tutup popup edit
        tutupPopup('editBayarPopup');
      } else {
        bukaEditBayar(_editBayarCurrentNama);
      }
      muatRekapPinjaman();
      muatRekapPegawai();
    }, 300);
    
  } catch (e) {
    showToast('❌ Gagal: ' + e.message);
  } finally {
    hideLoading();
    btn.innerHTML = '<i class="fa-solid fa-floppy-disk"></i><span>Simpan Perubahan</span>';
    btn.disabled = false;
  }
};

// ===== HAPUS BAYAR =====

window.hapusBayar = async (id) => {
  if (!id) {
    showToast('❌ ID tidak valid');
    return;
  }
  
  if (!confirm('Hapus pembayaran ini?\n\nSisa pinjaman akan bertambah kembali.\nTindakan ini tidak bisa dibatalkan.')) {
    return;
  }
  
  showLoading('Menghapus...');
  
  try {
    await sbDelete(sbDoc(db, COL.bayar, id));
    
    showToast('🗑 Pembayaran dihapus');
    tutupPopup('editNominalBayarPopup');
    
    // Refresh list
    setTimeout(() => {
      // Cek masih ada bayar?
      const sisaBayar = _CACHE.bayar.filter(b => b.nama === _editBayarCurrentNama && !b.luarGaji);
      if (sisaBayar.length === 0) {
        tutupPopup('editBayarPopup');
      } else {
        bukaEditBayar(_editBayarCurrentNama);
      }
      muatRekapPinjaman();
      muatRekapPegawai();
    }, 300);
    
  } catch (e) {
    showToast('❌ Gagal: ' + e.message);
  } finally {
    hideLoading();
  }
};

window.filterRekap = q => { if(currentTab==='pegawai') renderTabelRekap(q); else renderTabelPinjaman(q); };
window.toggleCheckAllRekap = cb => document.querySelectorAll('.row-check-rekap').forEach(el => el.checked=cb.checked);

window.bukaTambahanPopup = (nama) => {
  nama = nama || _rcNamaAktif;
  if(!nama){ showToast('Nama pegawai tidak ditemukan!'); return; }
  _rcNamaAktif = nama;

  // ✅ FIX 4: Load data tambahan dari Supabase jika _rcTambahan kosong
  if(!_rcTambahan[nama]) {
    const today = new Date();
    const day = today.getDay() || 7;
    const senin = new Date(today); senin.setDate(today.getDate() - day + 1); senin.setHours(0,0,0,0);
    const seninISO = senin.toISOString().slice(0,10);
    const minggu = new Date(senin); minggu.setDate(senin.getDate() + 6);
    const mingguISO = minggu.toISOString().slice(0,10);
    // Data sudah ada di _rcTambahan jika initDataListeners berhasil
    // Jika masih kosong, biarkan kosong (user akan input baru)
  }

  const peg = MASTER.pegawaiData[nama] || { bagian: '' };
  const bagian = (peg.bagian || '').trim();
  $('tamNama').textContent = nama;
  $('tamBagian').textContent = bagian;
  const allItems = MASTER.tambahanItemList || [];
  const items = allItems.filter(t => !t.bagian || t.bagian === bagian || bagian === '');
  const container = $('tamItemRows');
  const emptyMsg = $('tamEmptyMsg');
  if(!items.length) { container.innerHTML = ''; emptyMsg.style.display = 'block'; emptyMsg.innerHTML = `<div class="empty-state-box"><i class="fa-solid fa-circle-info"></i><div class="empty-state-box-text">Tidak ada item tambahan untuk bagian ini.<br><span style="font-size:11px;font-weight:500;">Tambahkan di menu <b>Harga → Tambahan</b>.</span></div></div>`; }
  else {
    emptyMsg.style.display = 'none';
    window._tamValues = {};
    const saved = _rcTambahan[nama] || {};
    const savedItems = saved.items || {};
    container.innerHTML = items.map((t, i) => {
      const key = _tamKey(t);
      const qty = savedItems[key] ? savedItems[key].qty : '';
      window._tamValues[i] = qty !== '' ? qty : 0;
      const harga = t.harga || 0;
      const satuan = (t.satuan || 'HARI').toLowerCase();
      const ph = harga > 0 ? `qty... (@ ${fmt(harga)})` : 'qty... (⚠ harga 0)';
      const fs = t.jenis.length > 14 ? '11px' : '13px';
      return `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;align-items:center;">
        <div style="background:linear-gradient(135deg, var(--primary3), var(--primary));color:#fff;border-radius:12px;padding:12px 10px;text-align:center;">
          <div style="font-size:${fs};font-weight:800;letter-spacing:.03em;line-height:1.2;">${t.jenis}</div>
          <div style="font-size:9px;font-weight:600;opacity:.8;margin-top:3px;">Rp ${fmt(harga)}/${satuan}</div>
        </div>
        <div class="popup-input-trigger" onclick="bukaNumpadGenTam(${i})" style="margin-top:0;">
          <input type="text" id="tamItem_${i}" data-idx="${i}" readonly
            placeholder="${ph}" inputmode="none"
            class="popup-input-no-keyboard"
            style="font-size:13px;padding:13px 14px;border-radius:12px;cursor:pointer;"
            value="${qty !== '' ? qty + ' ' + satuan : ''}">
        </div>
      </div>`;
    }).join('');
  }
  window._tamCurrentItems = items;
  hitungTotalTambahan();
  $('tambahanPopup').classList.add('active');

  // Keyboard push-up handler untuk tambahanPopup
  const card = document.querySelector('#tambahanPopup .rc-card');
  if (card && window.visualViewport) {
    const _tamVpHandler = () => {
      const kbH = window.innerHeight - window.visualViewport.height;
      if (kbH > 80) {
        card.style.transform = `translateY(-${kbH}px)`;
        const activeEl = document.activeElement;
        if (activeEl && activeEl.tagName === 'INPUT')
          setTimeout(() => activeEl.scrollIntoView({ behavior: 'smooth', block: 'center' }), 50);
      } else {
        card.style.transform = '';
      }
    };
    window.visualViewport.addEventListener('resize', _tamVpHandler);
    window.visualViewport.addEventListener('scroll', _tamVpHandler);
    $('tambahanPopup')._removeTamVp = () => {
      window.visualViewport.removeEventListener('resize', _tamVpHandler);
      window.visualViewport.removeEventListener('scroll', _tamVpHandler);
      card.style.transform = '';
    };
  }
};

window._tamValues = {}; // {index: qty}

window.hitungTotalTambahan = () => {
  const items = window._tamCurrentItems || [];
  let total = 0;
  items.forEach((t, i) => {
    let qty = parseInt(window._tamValues[i] ?? 0) || 0;
    // ✅ FIX 6: Validasi qty
    if(qty < 0) { qty = 0; window._tamValues[i] = 0; }
    const _sat = (t.satuan || 'HARI').toUpperCase();
    if(_sat === 'HARI' && qty > 31) { qty = 31; window._tamValues[i] = 31; showToast('⚠ Maksimal 31 hari!'); }
    if(!Number.isInteger(qty)) { qty = Math.floor(qty); window._tamValues[i] = qty; }
    total += qty * (t.harga || 0);
  });
  const el = $('tamTotalDisplay');
  if(total > 0) { el.textContent = 'Rp ' + fmt(total); el.style.color = 'var(--primary3)'; el.style.fontWeight = '800'; }
  else { el.textContent = 'nominal...'; el.style.color = 'var(--muted)'; el.style.fontWeight = '700'; }
};

window.simpanTambahanRincian = async () => {
  const nama = _rcNamaAktif;
  if(!nama){ showToast('Error: nama pegawai tidak ditemukan'); return; }
  const saveBtn = document.querySelector('#tambahanPopup button[onclick="simpanTambahanRincian()"]');
  if(saveBtn && saveBtn.disabled) return;
  // FIX 8: Konfirmasi jika sudah ada data
  const existingTam = _rcTambahan[nama];
  if(existingTam && existingTam.total > 0) {
    const tamTgl = existingTam._tanggalISO || '';
    const todayISO = new Date().toISOString().slice(0,10);
    if(tamTgl === todayISO) {
      if(!confirm(`${nama} sudah punya tambahan hari ini (Rp ${fmt(existingTam.total)}). Ganti?`)) {
        return;
      }
    }
  }
  if(saveBtn) { saveBtn.disabled = true; saveBtn.style.opacity = '0.5'; }
  const items = window._tamCurrentItems || [];
  let total = 0;
  const savedItems = {};
  items.forEach((t, i) => {
    const qty = parseInt(window._tamValues[i] ?? 0) || 0;
    if(qty > 0 && (t.harga || 0) === 0) { showToast(`⚠ Harga "${t.jenis}" belum diset!`); }
    const jumlah = qty * (t.harga || 0);
    total += jumlah;
    if(qty > 0) {
      const key = _tamKey(t);
      savedItems[key] = { jenis: t.jenis, harga: t.harga || 0, qty, jumlah, satuan: t.satuan || 'HARI' };
    }
  });
  const _todayISO = new Date().toISOString().slice(0,10);
  const _todayWeekId = _getWeekId(new Date());
  showLoading('Menyimpan data...');
  try {
    // ✅ FIX 5: Simpan ke Supabase DULU, baru update local state
    // Set sementara agar _saveTambahanDinamisFB bisa baca items
    _rcTambahan[nama] = { items: savedItems, total, _tanggalISO: _todayISO, _weekId: _todayWeekId };
    const saved = await _saveTambahanDinamisFB(nama);
    if(saved) {
      // ✅ Konfirmasi local state hanya jika Supabase sukses
      _rcTambahan[nama] = { items: savedItems, total, _tanggalISO: _todayISO, _weekId: _todayWeekId };
      _saveTambahanToSession();
      hideLoading();
      showToast(total > 0 ? '✅ Data tersimpan!' : '✅ Tersimpan (total Rp 0)');
    } else {
      // Rollback local state jika Supabase gagal
      delete _rcTambahan[nama];
      hideLoading();
    }
  } catch(e) {
    // Rollback local state jika exception
    delete _rcTambahan[nama];
    hideLoading();
    showToast('❌ Error: ' + e.message);
    if(saveBtn) { saveBtn.disabled = false; saveBtn.style.opacity = ''; }
    return;
  }
  if(saveBtn) { saveBtn.disabled = false; saveBtn.style.opacity = ''; }
  if($('tambahanPopup')._removeTamVp) $('tambahanPopup')._removeTamVp();
  $('tambahanPopup').classList.remove('active');
  renderTabelRekap();
  loadDashboardDana();
};

async function _saveTambahanDinamisFB(nama) {
  if(!nama || !_rcTambahan[nama]) return false;
  try {
    const docId = `${nama.replace(/\s+/g,'_')}_${new Date().toISOString().split('T')[0]}`;
    const tam = _rcTambahan[nama];
    const payload = { nama, total: tam.total || 0, tanggal: new Date(), tanggalISO: new Date().toISOString().slice(0,10), weekId: _getWeekId(new Date()), updatedAt: sbTimestamp(), itemsJSON: JSON.stringify(tam.items || {}) };
    const items = tam.items || {};
    const _findQty = (jenis) => { const e = Object.entries(items).find(([k]) => k.toUpperCase().endsWith('|' + jenis)); return e ? (e[1].qty || 0) : 0; };
    const _findJumlah = (jenis) => { const e = Object.entries(items).find(([k]) => k.toUpperCase().endsWith('|' + jenis)); return e ? (e[1].jumlah || 0) : 0; };
    payload.hariMakan = _findQty('UANG MAKAN');
    payload.jumlahMakan = _findJumlah('UANG MAKAN');
    payload.hariMakanMinggu = _findQty('UANG MAKAN MINGGU');
    payload.jumlahMakanMinggu = _findJumlah('UANG MAKAN MINGGU');
    payload.hariLembur = _findQty('LEMBUR');
    payload.jumlahLembur = _findJumlah('LEMBUR');
    payload.hariBonus = _findQty('BONUS');
    payload.jumlahBonus = _findJumlah('BONUS');
    await sbUpsert(sbDoc(db, 'tambahan', docId), payload, { merge: true });
    return true;
  } catch(e) { console.error('❌ Gagal simpan tambahan:', e); showToast('⚠️ Gagal simpan ke database: ' + e.message); return false; }
}

window.bukaSlipGaji = (nama) => {
  const dari = _getISO('rekapMulai');
  const sampai = _getISO('rekapAkhir');
  const data = getRincianLocal(nama, dari, sampai);
  
  if (!data.items.length && data.potongan === 0) {
    showToast('Tidak ada data untuk dicetak!');
    return;
  }

  // Header
  $('slipNama').textContent = nama;
  $('slipBagian').textContent = data.bagian || '-';
  $('slipPeriode').textContent = 'Periode: ' + (_tgl.rekapMulai || '--') + ' — ' + (_tgl.rekapAkhir || '--');

  let totalPekerjaan = 0;
  let totalTambahan = 0;
  let totalPotongan = 0;

  // === PEKERJAAN ===
  const pekerjaanContainer = $('slipPekerjaanList');
  pekerjaanContainer.innerHTML = '';
  
  if (data.items.length > 0) {
    // Merge same jenis+harga
    const merged = {};
    data.items.forEach(r => {
      const key = (r.jenis || '-') + '||' + (r.harga || 0);
      if (!merged[key]) merged[key] = { jenis: r.jenis, harga: r.harga, qty: 0, jumlah: 0, tanggals: [] };
      merged[key].qty += (r.qty || 0);
      merged[key].jumlah += (r.jumlah || 0);
      if (r.tanggalISO) merged[key].tanggals.push(r.tanggalISO);
    });

    Object.values(merged).forEach(p => {
      totalPekerjaan += p.jumlah;
      const tgl = p.tanggals.length > 0 ? p.tanggals[0].split('-').reverse().join('/') : '';
      const row = document.createElement('div');
      row.className = 'slip-item';
      row.innerHTML = `
        <div class="slip-item-left">
          <div class="slip-item-name">${p.jenis || '-'}</div>
          <div class="slip-item-meta">${p.qty} pcs × Rp ${fmt(p.harga)}${tgl ? ' • ' + tgl : ''}</div>
        </div>
        <div class="slip-item-right">
          <div class="slip-item-amt">Rp ${fmt(p.jumlah)}</div>
        </div>
      `;
      pekerjaanContainer.appendChild(row);
    });
  }

  $('slipSubPekerjaan').textContent = 'Rp ' + fmt(totalPekerjaan);

  // === TAMBAHAN ===
  const tam = _rcTambahan[nama];
  const tamTgl = tam?._tanggalISO || '';
  const tamDalamRange = (!dari || tamTgl >= dari) && (!sampai || tamTgl <= sampai);
  const secTambahan = $('slipSecTambahan');
  
  if (tam && tamDalamRange) {
    const _tamFromItemsSlip = tam.items ? Object.values(tam.items).reduce((s, it) => s + (it.jumlah > 0 ? it.jumlah : (it.qty||0)*(it.harga||0)), 0) : 0;
    const _tamTotalSlip = tam.total > 0 ? tam.total : _tamFromItemsSlip;
    if(_tamTotalSlip > 0) {
    secTambahan.style.display = '';
    const tambahanContainer = $('slipTambahanList');
    tambahanContainer.innerHTML = '';
    
    if (tam.items) {
      Object.values(tam.items).forEach(it => {
        const jumlahIt = it.jumlah > 0 ? it.jumlah : (it.qty||0)*(it.harga||0);
        if (jumlahIt > 0) {
          totalTambahan += jumlahIt;
          const row = document.createElement('div');
          row.className = 'slip-item';
          row.innerHTML = `
            <div class="slip-item-left">
              <div class="slip-item-name">${it.jenis}</div>
              <div class="slip-item-meta">${it.qty} ${(it.satuan||'HARI').toLowerCase()} × Rp ${fmt(it.harga)}</div>
              <div class="slip-item-meta" style="font-size:6px;color:#aaa;">${tam._tanggalISO || ''}</div>
            </div>
            <div class="slip-item-right">
              <div class="slip-item-amt g">+ Rp ${fmt(jumlahIt)}</div>
            </div>
          `;
          tambahanContainer.appendChild(row);
        }
      });
    }
    if(totalTambahan === 0 && _tamTotalSlip > 0) totalTambahan = _tamTotalSlip;
    $('slipSubTambahan').textContent = '+ Rp ' + fmt(totalTambahan);
    } else { secTambahan.style.display = 'none'; }
  } else {
    secTambahan.style.display = 'none';
  }

  // === SUBTOTAL KOTOR (Pekerjaan + Tambahan, sebelum Potongan) ===
  const totalKotorSementara = totalPekerjaan + totalTambahan;
  const subKotorWrap = $('slipSubKotorWrap');

  // === POTONGAN ===
  const allBayar = _CACHE.bayar.filter(b => {
    if (b.nama !== nama) return false;
    if (b.luarGaji) return false;
    if (dari && b.tanggalISO < dari) return false;
    if (sampai && b.tanggalISO > sampai) return false;
    return true;
  });

  const secPotongan = $('slipSecPotongan');
  if (allBayar.length > 0) {
    secPotongan.style.display = '';
    subKotorWrap.style.display = '';
    $('slipSubKotor').textContent = 'Rp ' + fmt(totalKotorSementara);
    const potonganContainer = $('slipPotonganList');
    potonganContainer.innerHTML = '';
    
    const pinjamanMap = {};
    allBayar.forEach(b => {
      const jenis = b.jenisPinjaman || 'Pinjaman';
      if (!pinjamanMap[jenis]) pinjamanMap[jenis] = 0;
      pinjamanMap[jenis] += (b.bayar || 0);
    });

    Object.entries(pinjamanMap).forEach(([jenis, jumlah]) => {
      totalPotongan += jumlah;
      const row = document.createElement('div');
      row.className = 'slip-item';
      row.innerHTML = `
        <div class="slip-item-left">
          <div class="slip-item-name">${jenis}</div>
        </div>
        <div class="slip-item-right">
          <div class="slip-item-amt r">- Rp ${fmt(jumlah)}</div>
        </div>
      `;
      potonganContainer.appendChild(row);
    });
    $('slipSubPotongan').textContent = '- Rp ' + fmt(totalPotongan);
  } else {
    secPotongan.style.display = 'none';
    subKotorWrap.style.display = 'none';
  }

  // === TOTAL BERSIH ===
  // SISTEM AKUMULASI: Total Pekerjaan + Tambahan - Potongan
  const totalKotor = totalPekerjaan + totalTambahan;
  const totalBersih = totalKotor - totalPotongan;

  $('slipTotalBersih').textContent = 'Rp ' + fmt(totalBersih);

  let detailParts = [];
  if (totalPekerjaan > 0) detailParts.push('Pekerjaan Rp ' + fmt(totalPekerjaan));
  if (totalTambahan > 0) detailParts.push('+ Tambahan Rp ' + fmt(totalTambahan));
  if (totalPotongan > 0) detailParts.push('- Potongan Rp ' + fmt(totalPotongan));
  $('slipTotalDetail').textContent = detailParts.join(' • ');

  // Footer date
  const now = new Date();
  $('slipFooterDate').textContent = 'Dicetak: ' + fmtTgl(now) + ' ' + 
    String(now.getHours()).padStart(2,'0') + ':' + String(now.getMinutes()).padStart(2,'0');

  $('slipPopup').classList.add('active');
};

window.tutupSlip = () => $('slipPopup').classList.remove('active');

window.cetakSlip = () => {
  // Dengan @media print, kita hanya perlu memanggil window.print()
  // Browser akan otomatis menerapkan gaya dari #print-style
  window.print();
};

// Stub awal agar _printMassalViaWindow tersedia sebelum cetakMassal dipanggil
window._printMassalViaWindow = function() { showToast('Buka Rekap dan pilih pegawai dulu'); };

window.cetakMassal = () => {
  
  const checkboxes = document.querySelectorAll('.row-check-rekap:checked');
  if(!checkboxes.length){ showToast('Pilih minimal satu pegawai!'); return; }
  
  const dari=_getISO('rekapMulai'), sampai=_getISO('rekapAkhir');
  let htmlGabungan='';
  
  // Alih-alih membuat HTML baru, kita akan mengkloning slip yang ada
  // dan menampilkannya di overlay cetak.
  const slipTemplate = $('slipBox');
  if (!slipTemplate) {
    showToast('❌ Template slip tidak ditemukan!');
    return;
  }
  
  const slipContainer = document.createElement('div');
  slipContainer.style.cssText = 'flex:1;overflow-y:auto;padding:16px;width:100%;display:flex;flex-direction:column;align-items:center;';
  
  checkboxes.forEach(cb => {
    const nama = cb.value;
    bukaSlipGaji(nama); // Buka slip untuk mengisi data
    const slipClone = slipTemplate.cloneNode(true);
    slipClone.style.marginBottom = '16px';
    slipClone.style.pageBreakAfter = 'always';
    slipContainer.appendChild(slipClone);
  });
  tutupSlip(); // Tutup popup slip yang terakhir dibuka

  if (!slipContainer.hasChildNodes()) {
    showToast('Tidak ada data untuk dicetak!');
    return;
  }

  const overlayId = '_printMassalOverlay';
  let overlay = document.getElementById(overlayId);
  if(overlay) overlay.remove();
  
  const printStyleId = '_printMassalStyle';
  document.getElementById(printStyleId)?.remove();
  
  const printStyleEl = document.createElement('style');
  printStyleEl.id = printStyleId;
  const _printMediaCSS = `
@media print {
  html, body { background:white!important; overflow:visible!important; height:auto!important; margin:0!important; padding:0!important; }
  body > * { display:none!important; }
  body > #_printMassalOverlay { display:block!important; position:static!important; overflow:visible!important; height:auto!important; width:100%!important; background:white!important; z-index:auto!important; margin:0!important; padding:0!important; }
  .print-btn-bar, ._pm-bar { display:none!important; }
  .slip-page {
    box-shadow:none!important; margin:0!important; padding:0!important;
    width:100%!important;
  }
  .slip-box {
    page-break-after: always;
  }
  .slip-page[data-last-slip], #_lastSlipPage { page-break-after:avoid!important; break-after:avoid!important; }
  #_printMassalOverlay > div { display:block!important; margin:0!important; padding:0!important; }
  @page { size:A6 portrait; margin:0; -webkit-print-color-adjust:exact; print-color-adjust:exact; }
}`;
  printStyleEl.textContent = _massalCSS + '\n' + _printMediaCSS;
  document.head.appendChild(printStyleEl);
  
  overlay = document.createElement('div');
  overlay.id = overlayId;
  overlay.style.cssText = 'position:fixed;inset:0;z-index:9999999;background:#F0F8FF;display:flex;flex-direction:column;';
  
  const bar = document.createElement('div');
  bar.className = '_pm-bar print-btn-bar';
  
  const btnCetak = document.createElement('button');
  btnCetak.textContent = '🖨 Cetak Semua';
  btnCetak.addEventListener('click', function() { window.print(); });
  
  const btnTutup = document.createElement('button');
  btnTutup.textContent = '✕ Tutup';
  btnTutup.style.cssText = 'margin-left:12px;background:#333333;';
  btnTutup.addEventListener('click', function() { 
    overlay.remove(); 
    const ps = document.getElementById('_printMassalStyle'); 
    if(ps) ps.remove();
    window.removeEventListener('beforeprint', _bpM);
    window.removeEventListener('afterprint', _apM);
    window.removeEventListener('beforeprint', _scaleSlips);
  });
  bar.appendChild(btnCetak);
  bar.appendChild(btnTutup);
  
  overlay.appendChild(slipContainer);
  overlay.appendChild(bar);
  
  const _allSlips = overlay.querySelectorAll('.slip-page');
  if(_allSlips.length > 0) { 
    _allSlips[_allSlips.length - 1].id = '_lastSlipPage'; 
  }
  
  document.body.appendChild(overlay);
  
  const _bpM = () => { if(bar.parentNode) bar.parentNode.removeChild(bar); };
  const _apM = () => { if(!overlay.contains(bar)) overlay.appendChild(bar); };
  window.addEventListener('beforeprint', _bpM);
  window.addEventListener('afterprint', _apM);
  
  // Keluar mode pilih setelah cetak
  batalModePilih();
};

const _slipAutoShrinkScript = `<script>(function(){
  var maxH=137,root=document.getElementById('slip-root');
  if(!root)return;
  function fit(){
    root.style.zoom=1;
    var hMM=root.offsetHeight*25.4/96;
    if(hMM>maxH){
      var s=maxH/hMM;
      root.style.zoom=s;
    }
  }
  window.addEventListener('load',function(){
    if(document.fonts&&document.fonts.ready){ document.fonts.ready.then(function(){ setTimeout(fit,100); }); }
    else { setTimeout(fit,300); }
  });
  window.addEventListener('beforeprint',fit);
})();<\/script>`;

function _mergeItemsForPrint(items) {
  const map = {};
  items.forEach(r => {
    const key = (r.jenis||'') + '||' + (r.harga||0);
    if(!map[key]) map[key] = { jenis:r.jenis, qty:0, harga:r.harga, jumlah:0 };
    map[key].qty += (r.qty||0);
    map[key].jumlah += (r.jumlah||0);
  });
  return Object.values(map);
}

// ===== DATE PICKER =====
window.bukaDatePicker = target => {
  dpTarget = target;
  const titles = { mulai:'Tanggal Mulai', akhir:'Tanggal Akhir', printMulai:'Tanggal Mulai', printAkhir:'Tanggal Akhir' };
  $('dpTitle').textContent = titles[target]||'Pilih Tanggal';
  const stateMap = { mulai:'rekapMulai', akhir:'rekapAkhir', printMulai:'printMulai', printAkhir:'printAkhir' };
  const label = _tgl[stateMap[target]]||'';
  if(label&&label.includes('/')) {
    const p=label.split('/');
    $('dpInput').value=`${p[2]}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`;
  } else $('dpInput').value='';
  $('datePickerPopup').classList.add('active');
};
window.tutupDatePicker = () => $('datePickerPopup').classList.remove('active');
window.konfirmasiDatePicker = () => {
  const val=$('dpInput').value;
  if(!val){ tutupDatePicker(); return; }
  const [y,m,d]=val.split('-');
  const stateMap = { mulai:'rekapMulai', akhir:'rekapAkhir', printMulai:'printMulai', printAkhir:'printAkhir' };
  _setTgl(stateMap[dpTarget], `${d}/${m}/${y}`);
  tutupDatePicker();
  if(dpTarget==='mulai'||dpTarget==='akhir') {
    if(currentTab==='pegawai') muatRekapPegawai();
    else muatRekapPinjaman();
  }
};

// ===== TOGGLE MINGGU =====
window.toggleMinggu = () => {
  _weekToggleOn = !_weekToggleOn;
  const track = document.getElementById('weekToggleTrack');
  const thumb = document.getElementById('weekToggleThumb');
  const label = document.getElementById('weekToggleLabel');
  const wrap = document.getElementById('weekToggleWrap');
  if (_weekToggleOn) {
    track.style.background = 'var(--primary)';
    thumb.style.left = '18px';
    label.style.color = 'var(--primary)';
    label.textContent = 'MINGGU INI';
    wrap.style.borderColor = 'var(--primary)';
    setPeriodeMingguIni();
  } else {
    track.style.background = 'var(--muted)';
    thumb.style.left = '2px';
    label.style.color = 'var(--muted)';
    label.textContent = 'MINGGU LALU';
    wrap.style.borderColor = 'var(--muted)';
    setPeriodeMingguLalu();
  }
};

window.setPeriodeMingguIni = () => {
  const hariIni = new Date();
  const senin = _getSenin(hariIni);
  _setTgl('rekapMulai', fmtTgl(senin));
  _setTgl('rekapAkhir', fmtTgl(hariIni));
  if(currentTab==='pegawai') muatRekapPegawai(); else muatRekapPinjaman();
};
window.setPeriodeMingguLalu = () => {
  const hariIni = new Date();
  const seninIni = _getSenin(hariIni);
  const seninLalu = new Date(seninIni);
  seninLalu.setDate(seninLalu.getDate() - 7);
  const mingguLalu = new Date(seninIni);
  mingguLalu.setDate(mingguLalu.getDate() - 1);
  _setTgl('rekapMulai', fmtTgl(seninLalu));
  _setTgl('rekapAkhir', fmtTgl(mingguLalu));
  if(currentTab==='pegawai') muatRekapPegawai(); else muatRekapPinjaman();
};

// ===== DASHBOARD =====
window.loadDashboardDana = () => {
  const dari=_getISO('rekapMulai'), sampai=_getISO('rekapAkhir');
  const items = getInputDataLocal(dari, sampai);
  let totalKotor=0;
  items.forEach(r => totalKotor+=(r.jumlah||0));
  const namaAktif = [...new Set(items.map(r=>r.nama))];
  let totalTambahan = 0;
  namaAktif.forEach(nama => {
    const tam = _rcTambahan[nama];
    if(tam && tam.total > 0) {
      const tamTgl = tam._tanggalISO || '';
      const inRange = (!dari || tamTgl >= dari) && (!sampai || tamTgl <= sampai);
      if(inRange) totalTambahan += tam.total;
    }
  });
  let totalPotongan=0;
  _CACHE.bayar.forEach(b => {
    if(!namaAktif.includes(b.nama)) return;
    if(dari && b.tanggalISO<dari) return;
    if(sampai && b.tanggalISO>sampai) return;
    totalPotongan += (b.bayar||0);
  });
  // Card biru: Total Dana Disiapkan (gaji + tambahan, sebelum potongan hutang)
  $('dashTotalDana').textContent = 'Rp '+fmt(totalKotor + totalTambahan);
  // Card hijau: Dana Bersih = (gaji + tambahan) - bayar hutang periode ini
  const danaBersih = Math.max(0, totalKotor + totalTambahan - totalPotongan);
  const dashDanaBersih = $('dashDanaBersih');
  if(dashDanaBersih) dashDanaBersih.textContent = 'Rp '+fmt(danaBersih);

  // Update home stats, pass totalPotongan untuk kolom merah
  _updateHomeStats(totalPotongan, totalTambahan, namaAktif.length);
};

function _updateHomeStats(totalPotonganPeriode, totalTambahanPeriode, pegawaiAktifCount) {
  const todayISO = new Date().toISOString().slice(0,10);
  const todayItems = _CACHE.input.filter(r => r.tanggalISO === todayISO);

  // Kolom merah: Bayar hutang periode ini (totalPotongan dari loadDashboardDana)
  const homeTotalPinj = $('homeStatTotalPinjaman');
  if(homeTotalPinj) homeTotalPinj.textContent = 'Rp ' + fmt(totalPotonganPeriode || 0);

  // Kolom oranye: Total Tambahan periode ini (dari loadDashboardDana)
  const homeTambahan = $('homeStatTotalTambahan');
  if(homeTambahan) homeTambahan.textContent = 'Rp ' + fmt(totalTambahanPeriode || 0);

  // Kolom biru: Pegawai aktif periode ini (dari loadDashboardDana)
  const homePegawai = $('homeStatPegawaiAktif');
  if(homePegawai) homePegawai.textContent = pegawaiAktifCount || 0;

  // Pinjaman aktif count (untuk badge warning & lunas)
  const pinjamanAktif = getRekapPinjamanLocal().filter(r => r.sisa > 0);

  // Warning & Lunas (pinjaman aktif count)
  const homeWarn = $('homeStatWarning');
  const homeLunas = $('homeStatLunas');
  if(homeWarn) homeWarn.textContent = pinjamanAktif.length;
  if(homeLunas) {
    const bebasPinjaman = MASTER.pegawai.length - pinjamanAktif.length;
    homeLunas.textContent = Math.max(0, bebasPinjaman);
  }

  // Update recent activity dengan data real
  const todayISO2 = new Date().toISOString().slice(0,10);
  const todayItems2 = _CACHE.input.filter(r => r.tanggalISO === todayISO2);
  _renderHomeRecentActivity(todayItems2);
}

function _renderHomeRecentActivity(todayItems) {
  const container = $('homeRecentActivity');
  if(!container) return;
  if(!todayItems || !todayItems.length) {
    container.innerHTML = '<div style="text-align:center;color:var(--muted);font-size:12px;padding:16px;font-weight:600;"><i class="fa-solid fa-inbox" style="display:block;font-size:24px;margin-bottom:8px;opacity:.3;"></i>Belum ada aktivitas hari ini</div>';
    return;
  }
  const sorted = [...todayItems].sort((a,b) => {
    const ta = a.waktu?.seconds || 0;
    const tb = b.waktu?.seconds || 0;
    return tb - ta;
  }).slice(0, 5);

  const icons = {
    'default': ['fa-keyboard', 'var(--primary3)', 'var(--primary)'],
  };

  container.innerHTML = sorted.map((r, i) => {
    const jml = Number(r.jumlah || 0);
    const jmlStr = fmt(jml);
    const colors = [['var(--primary3)','var(--primary)'], ['#059669','#10B981'], ['#D97706','#F59E0B'], ['#7C3AED','#8B5CF6'], ['#DC2626','#EF4444']];
    const [c1, c2] = colors[i % colors.length];
    return `<div style="display:flex;align-items:center;gap:10px;padding:8px 10px;background:var(--bg2);border-radius:12px;">
      <div style="width:32px;height:32px;border-radius:10px;background:linear-gradient(135deg,${c1},${c2});display:flex;align-items:center;justify-content:center;color:#fff;font-size:12px;flex-shrink:0;"><i class="fa-solid fa-briefcase"></i></div>
      <div style="flex:1;min-width:0;">
        <div style="font-size:12px;font-weight:700;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${r.nama || '-'}</div>
        <div style="font-size:9px;color:var(--muted);font-weight:600;">${r.jenis || '-'} · ${r.qty || 0} pcs · ${r.brand || '-'}</div>
      </div>
      <div style="font-size:11px;font-weight:800;color:var(--green);flex-shrink:0;">Rp ${jmlStr}</div>
    </div>`;
  }).join('');
}

// ===== QUICK STATS =====
window.refreshQuickStats = function() {
  const today = new Date().toISOString().slice(0,10);
  const todayLabel = new Date().toLocaleDateString('id-ID', { weekday:'long', day:'numeric', month:'long', year:'numeric' });
  const el = id => document.getElementById(id);
  if(!el('qs_tanggal')) return;
  const todayData = (_CACHE.input || []).filter(r => r.tanggalISO === today);
  const totalNominal = todayData.reduce((s, r) => s + (r.jumlah || 0), 0);
  const uniquePegawai = new Set(todayData.map(r => r.nama)).size;
  const newHash = todayData.length + '|' + totalNominal + '|' + (todayData[0]?.id || '');
  if(newHash === _qsLastHash && !_qsNeedsRefresh) return;
  _qsLastHash = newHash;
  _qsNeedsRefresh = false;
  el('qs_tanggal').textContent = todayLabel;
  el('qs_entri').textContent = todayData.length || '0';
  el('qs_pegawai').textContent = uniquePegawai || '0';
  const badgeEl = el('inpRiwayatBadge');
  if (badgeEl) {
    if (todayData.length > 0) {
      badgeEl.textContent = todayData.length;
      badgeEl.style.display = 'inline-block';
    } else {
      badgeEl.style.display = 'none';
    }
  }
  const totalStr = fmt(totalNominal);
  const totalEl = el('qs_total');
  if(totalEl) { totalEl.innerHTML = 'Rp ' + totalStr; }
  const recentEl = el('qs_recent');
  if(!recentEl) return;
  const sorted = [...todayData].sort((a, b) => {
    const ta = a.waktu?.seconds || 0;
    const tb = b.waktu?.seconds || 0;
    return tb - ta;
  });
  if(!sorted.length) {
    recentEl.innerHTML = '<div style="color:rgba(255,255,255,.6);font-size:11px;font-weight:600;text-align:center;padding:6px 0;">Belum ada data hari ini</div>';
    return;
  }
  recentEl.innerHTML = sorted.map(r => {
    const jml = Number(r.jumlah || 0);
    const jmlStr = 'Rp ' + fmt(jml);
    return '<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid rgba(255,255,255,.1);pointer-events:none;flex-shrink:0;">'
      + '<div style="flex:1;min-width:0;">'
      + '<div style="font-size:11px;font-weight:800;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + (r.nama || '-') + '</div>'
      + '<div style="font-size:9px;color:rgba(255,255,255,.7);font-weight:600;">' + (r.jenis || '-') + ' &middot; ' + (r.qty || 0) + ' pcs</div>'
      + '</div>'
      + '<div style="font-size:12px;font-weight:900;color:#fff;flex-shrink:0;margin-left:8px;">' + jmlStr + '</div>'
      + '</div>';
  }).join('');
  _fitQsRecent();
};

// Sembunyikan (bukan scroll) baris "Entri Terbaru" yang tidak muat
// di dalam kotak, supaya tetap terlihat rapi tanpa scroll internal.
window._fitQsRecent = function() {
  const box = document.getElementById('qs_recent');
  if(!box) return;
  const rows = Array.from(box.children);
  if(!rows.length) return;
  // Tampilkan dulu semua baris untuk diukur ulang dari awal
  rows.forEach(r => { r.style.display = ''; });
  const limitBottom = box.getBoundingClientRect().bottom;
  rows.forEach(r => {
    const rect = r.getBoundingClientRect();
    r.style.display = (rect.bottom <= limitBottom + 0.5) ? '' : 'none';
  });
};

// Perbesar kartu STATISTIK sampai mendekati bar "Kembali ke Menu Utama",
// agar memanfaatkan ruang kosong di bawahnya tanpa perlu scroll.
window._sizeQsCard = function() {
  const card = document.getElementById('qsCard');
  const panel = document.getElementById('inpPanelRiwayat');
  if(!card || !panel || panel.style.display === 'none') return;
  const backBar = document.querySelector('#inputPage .back-bar');
  const cardTop = card.getBoundingClientRect().top;
  const margin = 14; // jarak aman ke bar bawah
  const bottomLimit = backBar ? backBar.getBoundingClientRect().top : window.innerHeight;
  const h = bottomLimit - cardTop - margin;
  card.style.height = Math.max(h, 220) + 'px';
  _fitQsRecent();
};
window.addEventListener('resize', () => { if (document.getElementById('inputPage')?.classList.contains('active')) _sizeQsCard(); });

// ===== EDIT QTY =====
window.bukaEditQty = (id, currentQty, harga) => {
  $('edit_qty_id').value = id;
  $('edit_qty_harga').value = harga;
  $('edit_qty_val').value = currentQty;
  $('edit_qty_display').value = currentQty > 0 ? String(currentQty) : '';
  // Buka numpad langsung (popup Koreksi Qty belum ditampilkan) agar
  // tidak ada flash tampilan popup sebelum numpad muncul.
  bukaNumpadQty('edit');
};

window.simpanEditQty = async () => {
  const id = $('edit_qty_id').value;
  const harga = parseInt($('edit_qty_harga').value) || 0;
  const newQty = parseInt($('edit_qty_val').value) || 0;
  if (!id || newQty < 1) { showToast('Qty tidak valid!'); return; }
  showLoading('Memperbarui Qty...');
  try {
    await sbUpdate(sbDoc(db, COL.input, id), { qty: newQty, jumlah: newQty * harga });
    const _it = _CACHE.input.find(r => r.id === id);
    if (_it) { _it.qty = newQty; _it.jumlah = newQty * harga; }
    showToast('Qty berhasil diperbarui!');
    tutupPopup('editQtyPopup');
    if (typeof tutupDetailPekerjaan === 'function') tutupDetailPekerjaan();
    if (currentTab === 'pegawai') muatRekapPegawai();
  } catch(e) { showToast('Gagal: ' + e.message); }
  finally { hideLoading(); }
};

// ===== EDIT PINJAMAN =====
window.bukaEditPinjaman = nama => {
  const pinDocs = _CACHE.pinjaman.filter(p => p.nama === nama);
  if(!pinDocs.length){ showToast('Tidak ada data pinjaman untuk ' + nama); return; }
  if(pinDocs.length === 1) { _bukaFormEditPinjaman(pinDocs[0]); }
  else {
    window._pinEditList = pinDocs;
    const listHtml = pinDocs.map((p, i) =>
      `<button onclick="window._bukaFormEditPinjaman(window._pinEditList[${i}])" style="width:100%;text-align:left;background:var(--card2);border:1.5px solid var(--muted);border-radius:12px;padding:13px 16px;margin-bottom:8px;cursor:pointer;font-family:inherit;font-size:14px;font-weight:700;color:var(--text);display:flex;justify-content:space-between;align-items:center;">
        <span>${p.jenis||'(lainnya)'}</span>
        <span style="color:var(--primary3);font-weight:900;">${fmt(p.nominal||0)}</span>
      </button>`
    ).join('');
    const overlay = document.createElement('div');
    overlay.id = 'pilihJenisPinjaman';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:99999999;display:flex;align-items:flex-end;justify-content:center;padding:16px;';
    overlay.innerHTML = `<div style="background:var(--card);border-radius:20px;padding:20px;width:100%;max-width:420px;max-height:80vh;overflow-y:auto;">
      <div style="font-size:13px;font-weight:800;color:var(--muted);letter-spacing:.08em;text-transform:uppercase;margin-bottom:12px;">Pilih Jenis Pinjaman — ${nama}</div>
      ${listHtml}
      <button onclick="document.getElementById('pilihJenisPinjaman').remove()" style="width:100%;background:transparent;color:var(--muted);border:1.5px solid var(--muted);border-radius:12px;padding:12px;font-size:13px;font-weight:700;cursor:pointer;font-family:inherit;margin-top:4px;">Batal</button>
    </div>`;
    document.body.appendChild(overlay);
  }
};

window._bukaFormEditPinjaman = p => {
  const overlay = document.getElementById('pilihJenisPinjaman');
  if(overlay) overlay.remove();
  if(!p || !p.id) { showToast('❌ Data pinjaman tidak valid'); return; }
  document.getElementById('ep_id').value = p.id;
  document.getElementById('ep_nama').value = p.nama;
  document.getElementById('ep_jenis').value = p.jenis || '(lainnya)';
  const nom = p.nominal || 0;
  document.getElementById('ep_nominal').value = nom;
  document.getElementById('ep_nominal_display').value = nom > 0 ? 'Rp ' + fmt(nom) : '';
  bukaPopup('editPinjamanPopup');
};

window.simpanEditPinjaman = async () => {
  const id = document.getElementById('ep_id').value;
  const nominal = parseInt(document.getElementById('ep_nominal').value) || 0;
  if(!id || id.trim() === ''){ showToast('ID tidak valid'); return; }
  if(nominal < 0){ showToast('Nominal tidak boleh negatif'); return; }
  showLoading('Menyimpan perubahan...');
  try {
    await sbUpdate(sbDoc(db, COL.pinjaman, id), { nominal });
    tutupPopup('editPinjamanPopup');
    showToast('✅ Pinjaman berhasil diperbarui');
    setTimeout(() => { muatRekapPinjaman(); }, 300);
  } catch(e) { showToast('❌ Gagal: ' + e.message); }
  finally { hideLoading(); }
};

window.konfirmasiHapusPinjaman = () => {
  const id = document.getElementById('ep_id').value;
  const nama = document.getElementById('ep_nama').value;
  const jenis = document.getElementById('ep_jenis').value;
  if(!id) return;
  if(!confirm(`Hapus pinjaman "${jenis}" milik ${nama}?\n\nSemua riwayat pembayaran terkait juga akan dihapus.\nTindakan ini tidak bisa dibatalkan.`)) return;
  hapusPinjamanById(id);
};

window.hapusPinjamanById = async id => {
  if(!id || id.trim() === '') { showToast('❌ ID pinjaman tidak valid'); return; }
  showLoading('Menghapus pinjaman...');
  try {
    const pinDoc = _CACHE.pinjaman.find(p => p.id === id);
    await sbDelete(sbDoc(db, COL.pinjaman, id));
    if(pinDoc) {
      const relatedBayar = _CACHE.bayar.filter(b => b.nama === pinDoc.nama && b.jenisPinjaman === (pinDoc.jenis || '(lainnya)'));
      for(const b of relatedBayar) { await sbDelete(sbDoc(db, COL.bayar, b.id)); }
    }
    tutupPopup('editPinjamanPopup');
    showToast('🗑 Pinjaman & riwayat bayar berhasil dihapus');
    setTimeout(() => { muatRekapPinjaman(); muatRekapPegawai(); }, 300);
  } catch(e) { showToast('❌ Gagal hapus: ' + e.message); }
  finally { hideLoading(); }
}

// ===== HAPUS PINJAMAN DARI REKAP =====
window.hapusPinjamanRekap = (nama) => {
  if(!nama) { showToast('❌ Nama pegawai tidak valid'); return; }

  // Find pinjaman records for this user
  const pinjamanList = _CACHE.pinjaman.filter(p => p.nama === nama);
  if(!pinjamanList.length) { showToast('ℹ Tidak ada pinjaman untuk ' + nama); return; }

  // Sort by date (newest first)
  pinjamanList.sort((a, b) => (b.tanggalISO || '').localeCompare(a.tanggalISO || ''));

  // If only one pinjaman, show confirmation directly
  if(pinjamanList.length === 1) {
    const p = pinjamanList[0];
    bukaConfirm({
      icon: '<i class="fa-solid fa-triangle-exclamation"></i>',
      iconBg: '#FEE2E2', 
      iconColor: '#EF4444',
      title: 'Hapus Pinjaman?',
      msg: `<div style="text-align:left;line-height:1.6;">
        <b>${nama}</b><br><br>
        Jenis: <b>${p.jenis || 'Pinjaman'}</b><br>
        Nominal: <b style="color:#DC2626;">Rp ${fmt(p.nominal || 0)}</b><br>
        Tanggal: ${p.tanggal || '-'}<br><br>
        <span style="color:#DC2626;font-size:12px;">⚠ Data akan dihapus permanen dan tidak bisa dikembalikan!</span>
      </div>`,
      okLabel: 'Ya, Hapus Pinjaman',
      okBg: 'linear-gradient(135deg,#DC2626,#EF4444)',
      callback: () => {
        showLoading('Menghapus pinjaman...');
        sbDelete(sbDoc(db, COL.pinjaman, p.id))
          .then(() => {
            showToast('🗑 Pinjaman berhasil dihapus');
            setTimeout(() => {
              muatRekapPinjaman();
              muatRekapPegawai();
              loadDashboardDana();
            }, 300);
          })
          .catch(e => showToast('❌ Gagal: ' + e.message))
          .finally(() => hideLoading());
      }
    });
    return;
  }

  // Multiple pinjaman: show selection dialog
  const q = s => String(s).replace(/'/g,"\'");
  const listHtml = pinjamanList.map((p, i) => {
    const sisa = (p.nominal || 0) - (_CACHE.bayar
      .filter(b => b.nama === nama && b.jenisPinjaman === (p.jenis || 'Pinjaman') && !b.luarGaji)
      .reduce((s, b) => s + (b.bayar || 0), 0));
    return `<button onclick="window._hapusPinjamanTerpilih('${q(p.id)}', '${q(nama)}', '${q(p.jenis || 'Pinjaman')}', ${p.nominal || 0})" 
      style="width:100%;text-align:left;background:var(--card2);border:1.5px solid var(--muted);border-radius:12px;padding:13px 16px;margin-bottom:8px;cursor:pointer;font-family:inherit;display:flex;justify-content:space-between;align-items:center;transition:all .15s;"
      onmouseover="this.style.borderColor='var(--red)';this.style.background='#FEE2E2';"
      onmouseout="this.style.borderColor='var(--muted)';this.style.background='var(--card2)';">
      <div>
        <div style="font-size:13px;font-weight:800;color:var(--text);">${p.jenis || 'Pinjaman'}</div>
        <div style="font-size:10px;color:var(--muted);margin-top:2px;">${p.tanggal || '-'} • Sisa: Rp ${fmt(Math.max(0, sisa))}</div>
      </div>
      <div style="font-size:14px;font-weight:900;color:#DC2626;">Rp ${fmt(p.nominal || 0)}</div>
    </button>`;
  }).join('');

  bukaConfirm({
    icon: '<i class="fa-solid fa-list-check"></i>',
    iconBg: '#E0EAFF', 
    iconColor: '#1A56DB',
    title: 'Pilih Pinjaman',
    msg: `<div style="text-align:left;"><b>${nama}</b> memiliki ${pinjamanList.length} pinjaman aktif. Pilih yang akan dihapus:<br><br>${listHtml}</div>`,
    okLabel: 'Batal',
    okBg: 'linear-gradient(135deg,var(--muted),#9CA3AF)',
    callback: () => {} // Just close
  });
};

window._hapusPinjamanTerpilih = (id, nama, jenis, nominal) => {
  tutupConfirm();
  setTimeout(() => {
    bukaConfirm({
      icon: '<i class="fa-solid fa-triangle-exclamation"></i>',
      iconBg: '#FEE2E2', 
      iconColor: '#EF4444',
      title: 'Hapus Pinjaman Ini?',
      msg: `<div style="text-align:left;line-height:1.6;">
        <b>${nama}</b><br><br>
        Jenis: <b>${jenis}</b><br>
        Nominal: <b style="color:#DC2626;">Rp ${fmt(nominal)}</b><br><br>
        <span style="color:#DC2626;font-size:12px;">⚠ Data akan dihapus permanen dan tidak bisa dikembalikan!</span>
      </div>`,
      okLabel: 'Ya, Hapus',
      okBg: 'linear-gradient(135deg,#DC2626,#EF4444)',
      callback: () => {
        showLoading('Menghapus pinjaman...');
        sbDelete(sbDoc(db, COL.pinjaman, id))
          .then(() => {
            showToast('🗑 Pinjaman berhasil dihapus');
            setTimeout(() => {
              muatRekapPinjaman();
              muatRekapPegawai();
              loadDashboardDana();
            }, 300);
          })
          .catch(e => showToast('❌ Gagal: ' + e.message))
          .finally(() => hideLoading());
      }
    });
  }, 300);
};;

window.downloadSlipGaji = async (nama) => {
  const slipBox = $('slipBox');
  if (!slipBox) { showToast('Gagal: elemen slip tidak ditemukan'); return; }

  showLoading('Generating JPEG...');

  // Clone offscreen dengan ukuran A6 fixed agar hasil konsisten
  const W = 397; // 105mm @ 96dpi
  const H = 559; // 148mm @ 96dpi
  const clone = slipBox.cloneNode(true);
  clone.style.cssText = `
    position:absolute; left:-10000px; top:0;
    width:${W}px; height:${H}px; max-height:${H}px;
    border-radius:0; overflow:hidden;
    background:#fff; z-index:-1;
    display:flex; flex-direction:column;
  `;
  document.body.appendChild(clone);
  await new Promise(r => setTimeout(r, 80));

  try {
    const canvas = await html2canvas(clone, {
      scale: 3,
      useCORS: true,
      backgroundColor: '#FFFFFF',
      logging: false,
      width: W,
      height: H
    });

    const link = document.createElement('a');
    const tglLabel = (_tgl.rekapMulai || 'slip').replace(/\//g, '-');
    link.download = `Slip_${nama}_${tglLabel}.jpg`;
    link.href = canvas.toDataURL('image/jpeg', 0.92);
    link.click();
    showToast('✅ Download Berhasil!');
  } catch (e) {
    showToast('Gagal Download: ' + e.message);
  } finally {
    document.body.removeChild(clone);
    hideLoading();
  }
};

// ===== RAWBT THERMAL PRINTER INTEGRATION =====
/**
 * Cetak slip gaji ke printer thermal via RawBT
 * @param {string} nama - Nama pegawai
 * @param {Object} data - Data slip gaji
 * @param {string} periode - String periode
 */
function cetakRawBT(nama, data, periode) {
  const escpos = generateESCPOS(nama, data, periode);
  const base64Data = btoa(unescape(encodeURIComponent(escpos)));
  window.location.href = 'rawbt:base64,' + base64Data;
}

/**
 * Lebar kertas thermal 58mm = 32 karakter per baris (font normal).
 */
const PAPER_WIDTH = 32;

/**
 * Bersihkan karakter yang tidak ada di charset printer thermal
 * (em dash, en dash, smart quotes, dll) agar tidak tercetak sebagai
 * karakter aneh/kotak.
 */
function _cleanText(str) {
  return String(str ?? '')
    .replace(/[\u2012-\u2015\u2212]/g, '-')   // semua jenis dash -> "-"
    .replace(/[\u2018\u2019]/g, "'")          // smart single quote
    .replace(/[\u201C\u201D]/g, '"')          // smart double quote
    .replace(/\u2026/g, '...')                // ellipsis
    .replace(/[^\x00-\x7E]/g, '?');           // sisanya yg non-ASCII jadi "?"
}

/**
 * Baris dua kolom: teks kiri rata kiri, teks kanan rata kanan,
 * dengan padding spasi manual (bukan command alignment ESC/POS).
 */
function _row2(left, right, width = PAPER_WIDTH) {
  left = _cleanText(left);
  right = _cleanText(right);
  const space = width - left.length - right.length;
  if (space <= 1) {
    // Kalau kepanjangan, taruh kanan di baris baru, rata kanan
    const pad = Math.max(0, width - right.length);
    return left + '\x0A' + ' '.repeat(pad) + right;
  }
  return left + ' '.repeat(space) + right;
}

/** Garis pembatas penuh lebar kertas */
function _line(ch = '-', width = PAPER_WIDTH) {
  return ch.repeat(width);
}

/** Teks rata tengah secara manual (presisi, tidak bergantung command ESC a) */
function _center(text, width = PAPER_WIDTH) {
  text = _cleanText(text);
  if (text.length >= width) return text;
  const pad = Math.floor((width - text.length) / 2);
  return ' '.repeat(pad) + text;
}

/**
 * Generate ESC/POS commands untuk slip gaji (58mm / 32 kolom)
 */
function generateESCPOS(nama, data, periode) {
  const ESC = '\x1B', GS = '\x1D', LF = '\x0A';
  const BOLD_ON = ESC + 'E\x01', BOLD_OFF = ESC + 'E\x00';
  const DOUBLE_ON = ESC + '!\x30', DOUBLE_OFF = ESC + '!\x00';
  const CUT = GS + 'V\x01', INIT = ESC + '@';

  nama = _cleanText(nama);
  periode = _cleanText(periode);

  let out = INIT;

  // ===== HEADER =====
  out += BOLD_ON + DOUBLE_ON;
  out += _center('RADJA PRODUCTION', 16) + LF;
  out += DOUBLE_OFF + BOLD_OFF;
  out += _center('* SLIP GAJI KARYAWAN *') + LF;
  out += _line('=') + LF;

  // ===== INFO PEGAWAI =====
  out += BOLD_ON + 'Nama   ' + BOLD_OFF + ': ' + nama + LF;
  out += BOLD_ON + 'Bagian ' + BOLD_OFF + ': ' + (_cleanText(data.bagian) || '-') + LF;
  out += BOLD_ON + 'Periode' + BOLD_OFF + ': ' + LF;
  out += '  ' + periode + LF;
  out += _line('-') + LF;

  // ===== PEKERJAAN =====
  if (data.items?.length > 0) {
    out += BOLD_ON + '>> PEKERJAAN' + BOLD_OFF + LF;
    data.items.forEach(item => {
      const jenis = _cleanText(item.jenis || '-').substring(0, PAPER_WIDTH);
      const rincian = (item.qty || 0) + ' x Rp ' + fmt(item.harga || 0);
      out += ' ' + jenis + LF;
      out += _row2('   ' + rincian, 'Rp ' + fmt(item.jumlah || 0)) + LF;
    });
    out += _line('-') + LF;
    out += BOLD_ON + _row2('Subtotal', 'Rp ' + fmt(data.subtotal)) + BOLD_OFF + LF;
  }

  // ===== TAMBAHAN =====
  if (data.tamTotal > 0) {
    out += _line('-') + LF;
    out += BOLD_ON + '>> TAMBAHAN' + BOLD_OFF + LF;
    out += _row2('Total Tambahan', '+Rp ' + fmt(data.tamTotal)) + LF;
  }

  // ===== POTONGAN =====
  if (data.potongan > 0) {
    out += _line('-') + LF;
    out += BOLD_ON + '>> POTONGAN' + BOLD_OFF + LF;
    out += _row2('Total Potongan', '-Rp ' + fmt(data.potongan)) + LF;
  }

  // ===== TOTAL BERSIH =====
  out += _line('=') + LF;
  out += BOLD_ON + DOUBLE_ON;
  out += _center('TOTAL BERSIH', 16) + LF;
  out += _center('Rp ' + fmt(data.total), 16) + LF;
  out += DOUBLE_OFF + BOLD_OFF;
  out += _line('=') + LF;

  // ===== FOOTER =====
  out += _center('Dicetak: ' + _cleanText(new Date().toLocaleString('id-ID'))) + LF;
  out += _center('~ Terima kasih ~') + LF;
  out += LF + LF + LF + CUT;

  return out;
}

// ===== WRAPPER UNTUK SLIP POPUP =====
window.cetakSlipRawBT = function() {
  const nama = document.getElementById('slipNama').textContent;
  const periode = document.getElementById('slipPeriode').textContent.replace('Periode: ', '');
  const dari = _getISO('rekapMulai');
  const sampai = _getISO('rekapAkhir');
  const data = getRincianLocal(nama, dari, sampai);

  // Ambil tambahan
  const tam = _rcTambahan[nama];
  const tamTgl = tam?._tanggalISO || '';
  const tamDalamRange = (!dari || tamTgl >= dari) && (!sampai || tamTgl <= sampai);
  const tamTotalFromItems = tam?.items ? Object.values(tam.items).reduce((s, it) => s + (it.jumlah||0), 0) : 0;
  data.tamTotal = tamDalamRange ? (tam?.total > 0 ? tam.total : tamTotalFromItems) : 0;

  cetakRawBT(nama, data, periode);
};

// ===== CETAK MASSAL RAWBT =====
window.cetakMassalRawBT = function() {
  const checkboxes = document.querySelectorAll('.row-check-rekap:checked');
  if (!checkboxes.length) { showToast('Pilih minimal satu pegawai!'); return; }

  const dari = _getISO('rekapMulai'), sampai = _getISO('rekapAkhir');
  const periode = (_tgl.rekapMulai || '--') + ' - ' + (_tgl.rekapAkhir || '--');
  let combined = '';
  let count = 0;

  checkboxes.forEach((cb) => {
    const nama = cb.value;
    if (!nama?.trim() || nama === '-') return;

    const data = getRincianLocal(nama, dari, sampai);

    // Hitung tambahan (sama persis dengan cetakSlipRawBT)
    const tam = _rcTambahan[nama];
    const tamTgl = tam?._tanggalISO || '';
    const tamDalamRange = (!dari || tamTgl >= dari) && (!sampai || tamTgl <= sampai);
    const tamTotalFromItems = tam?.items
      ? Object.values(tam.items).reduce((s, it) => s + (it.jumlah||0), 0)
      : 0;
    data.tamTotal = tamDalamRange
      ? (tam?.total > 0 ? tam.total : tamTotalFromItems)
      : 0;

    if (!data.items.length && !data.tamTotal && !(data.potongan > 0)) return;

    if (count > 0) {
      // Separator antar slip
      combined += '\x1B' + 'a\x01' + '---\x0A\x0A';
    }
    combined += generateESCPOS(nama, data, periode);
    count++;
  });

  if (!count) { showToast('Tidak ada data untuk dicetak!'); return; }

  showToast('🖨 Mengirim ' + count + ' slip ke printer thermal…');
  window.location.href = 'rawbt:base64,' + btoa(unescape(encodeURIComponent(combined)));
};

// ===== PRINT MARGIN =====
const _PM_KEY = 'radja_print_margins';
const _PM_DEFAULT = { top: 4, left: 8, right: 8 };
function _loadPrintMarginsToUI() {
  try {
    const saved = JSON.parse(localStorage.getItem(_PM_KEY) || '{}');
    const top = (saved.top !== undefined) ? saved.top : _PM_DEFAULT.top;
    const left = (saved.left !== undefined) ? saved.left : _PM_DEFAULT.left;
    const right = (saved.right !== undefined) ? saved.right : _PM_DEFAULT.right;
    const topEl = document.getElementById('pm_top');
    const leftEl = document.getElementById('pm_left');
    const rightEl = document.getElementById('pm_right');
    if(topEl) { topEl.value = top; document.getElementById('pm_top_val').textContent = top; }
    if(leftEl) { leftEl.value = left; document.getElementById('pm_left_val').textContent = left; }
    if(rightEl) { rightEl.value = right; document.getElementById('pm_right_val').textContent = right; }
    _updatePrintPreview();
  } catch(e) {}
}
function _updatePrintPreview() {
  const top = parseInt(document.getElementById('pm_top')?.value ?? _PM_DEFAULT.top);
  const left = parseInt(document.getElementById('pm_left')?.value ?? _PM_DEFAULT.left);
  const right = parseInt(document.getElementById('pm_right')?.value ?? _PM_DEFAULT.right);
  const el = document.getElementById('pm_preview_code');
  if(el) el.innerHTML = `size: 100mm auto;<br>margin: ${top}mm ${right}mm 2mm ${left}mm;`;
}
window.updatePrintMarginLabel = function(side, val) {
  const el = document.getElementById('pm_' + side + '_val');
  if(el) el.textContent = val;
  _updatePrintPreview();
};
window.simpanPrintMargins = function() {
  const top = parseInt(document.getElementById('pm_top')?.value ?? _PM_DEFAULT.top);
  const left = parseInt(document.getElementById('pm_left')?.value ?? _PM_DEFAULT.left);
  const right = parseInt(document.getElementById('pm_right')?.value ?? _PM_DEFAULT.right);
  localStorage.setItem(_PM_KEY, JSON.stringify({ top, left, right }));
  showToast('✅ Pengaturan cetak disimpan!');
};
window.resetPrintMargins = function() {
  localStorage.removeItem(_PM_KEY);
  const pm = _PM_DEFAULT;
  const topEl = document.getElementById('pm_top');
  const leftEl = document.getElementById('pm_left');
  const rightEl = document.getElementById('pm_right');
  if(topEl) { topEl.value = pm.top; document.getElementById('pm_top_val').textContent = pm.top; }
  if(leftEl) { leftEl.value = pm.left; document.getElementById('pm_left_val').textContent = pm.left; }
  if(rightEl) { rightEl.value = pm.right; document.getElementById('pm_right_val').textContent = pm.right; }
  _updatePrintPreview();
  showToast('↺ Margin direset ke default');
};

// ===== CONFIRM =====
window.bukaConfirm = ({ icon, iconBg, iconColor, title, msg, okLabel, okBg, callback }) => {
  const el = id => document.getElementById(id);
  el('confirmIcon').innerHTML = icon;
  el('confirmIcon').style.background = iconBg || 'var(--primaryDim)';
  el('confirmIcon').style.color = iconColor || 'var(--primary3)';
  el('confirmTitle').textContent = title;
  el('confirmMsg').innerHTML = msg;
  el('confirmBtnOk').textContent = okLabel || 'Ya, Lanjutkan';
  el('confirmBtnOk').style.background = okBg || 'linear-gradient(135deg, var(--primary3), var(--primary))';
  el('confirmBtnOk').style.boxShadow = '0 4px 16px rgba(0,180,216,.28)';
  _confirmCallback = callback;
  el('confirmBtnOk').onclick = () => { const cb = _confirmCallback; tutupConfirm(); if(cb) setTimeout(cb, 200); };
  el('confirmOverlay').classList.add('active');
};
window.tutupConfirm = () => { document.getElementById('confirmOverlay').classList.remove('active'); _confirmCallback = null; };

// ===== ANDROID BACK =====
window.addEventListener('popstate', (e) => {
  const pages = ['inputPage', 'settingPage', 'hargaPage', 'rekapPage'];
  const activePage = pages.find(id => {
    const el = document.getElementById(id);
    return el && el.classList.contains('active');
  });
  if (activePage) {
    if (activePage === 'inputPage' && _stagingList && _stagingList.length > 0) {
      // Push state kembali dulu agar posisi history tidak berubah
      history.pushState({ page: 'inputPage' }, '');
      bukaConfirm({
        icon: '<i class="fa-solid fa-triangle-exclamation"></i>',
        iconBg: '#FEF3C7',
        iconColor: '#D97706',
        title: 'Data Belum Disimpan',
        msg: `Ada ${_stagingList.length} item yang belum disimpan. Yakin mau keluar? Data akan hilang.`,
        okLabel: 'Ya, Keluar',
        okBg: 'linear-gradient(135deg, #EF4444, #F87171)',
        callback: _tutupInputDataLangsung
      });
      return;
    }
    document.getElementById(activePage).classList.remove('active');
    if (activePage === 'rekapPage') try { loadDashboardDana(); } catch(e2) {}
    history.replaceState({ page: 'home' }, '');
    return;
  }
  history.pushState({ page: 'home' }, '');
});

// ===== PWA INSTALL =====
let _pwaInstallPrompt = null;
const _PWA_SKIP_KEY = 'pwa_install_skipped';

function _showInstallModal() {
  const modal = document.getElementById('pwaInstallModal');
  if(modal) {
    modal.style.display = 'flex';
    requestAnimationFrame(() => modal.classList.add('active'));
  }
}
function _hideInstallModal() {
  const modal = document.getElementById('pwaInstallModal');
  if(modal) {
    modal.classList.remove('active');
    setTimeout(() => { modal.style.display = 'none'; }, 300);
  }
}

window.addEventListener('beforeinstallprompt', (e) => {
  e.preventDefault();
  _pwaInstallPrompt = e;

  // Tampilkan tombol kecil di header
  const btn = $('btnInstallApp');
  if(btn) btn.style.display = 'flex';

  // Tampilkan modal otomatis setelah 2 detik, kecuali user pernah skip
  const skipped = localStorage.getItem(_PWA_SKIP_KEY);
  if(!skipped) {
    setTimeout(_showInstallModal, 2000);
  }
});

window.addEventListener('appinstalled', () => {
  _pwaInstallPrompt = null;
  const btn = $('btnInstallApp');
  if(btn) btn.style.display = 'none';
  _hideInstallModal();
  localStorage.removeItem(_PWA_SKIP_KEY);
});

// Tombol install kecil di header
document.getElementById('btnInstallApp')?.addEventListener('click', async () => {
  if(!_pwaInstallPrompt) return;
  _pwaInstallPrompt.prompt();
  const { outcome } = await _pwaInstallPrompt.userChoice;
  if(outcome === 'accepted') _pwaInstallPrompt = null;
});

// Tombol install di modal
document.getElementById('pwaInstallBtn')?.addEventListener('click', async () => {
  _hideInstallModal();
  if(!_pwaInstallPrompt) return;
  _pwaInstallPrompt.prompt();
  const { outcome } = await _pwaInstallPrompt.userChoice;
  if(outcome === 'accepted') _pwaInstallPrompt = null;
});

// Tombol skip di modal — simpan ke localStorage agar tidak muncul lagi
document.getElementById('pwaSkipBtn')?.addEventListener('click', () => {
  localStorage.setItem(_PWA_SKIP_KEY, '1');
  _hideInstallModal();
});

// ===== INIT =====
(function init() {
  const hariIni = new Date();
  const dSenin = new Date(hariIni);
  const hari = dSenin.getDay()||7;
  dSenin.setDate(dSenin.getDate()-hari+1);
  _setTgl('rekapMulai', fmtTgl(dSenin));
  _setTgl('rekapAkhir', fmtTgl(hariIni));
  _setTgl('printMulai', fmtTgl(dSenin));
  _setTgl('printAkhir', fmtTgl(hariIni));
  _initTimeout = setTimeout(()=>{
    const flags = Object.entries(_initFlags).filter(([,v])=>!v).map(([k])=>k);
    _showDbError('⚠ Koneksi timeout.<br>Tabel belum merespons: <b>' + flags.join(', ') + '</b><br><br>Kemungkinan penyebab:<br>• RLS Policy Supabase belum mengizinkan akses<br>• Tidak ada koneksi internet<br><br>Coba refresh halaman.');
  }, 12000);
  _initWeeklyReset();
  _loadTambahanFromSession();
  initMasterListener();
  initDataListeners();
  history.replaceState({ page: 'home' }, '');
})();