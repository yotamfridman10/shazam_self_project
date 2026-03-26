from wave import open as open_wave
from numpy import frombuffer, array, mean, std, int16
from hashlib import sha1
from collections import defaultdict
from fft import fft, volume, make_power_of_2
from db import get_matches_for_hashes, insert_many_fingerprints_copy


def open_song_wav(song_file):
    with open_wave(song_file, "rb") as wf:
        n_channels = wf.getnchannels()
        sample_width = wf.getsampwidth() 
        sr = wf.getframerate()
        n_frames = wf.getnframes() # song time (sec) = n_frames / sr 

        frames = wf.readframes(n_frames)

    samples = frombuffer(frames, dtype = int16)

    if n_channels == 2:
        samples = samples.reshape(-1, 2)
        samples = samples.mean(axis = 1)

    return samples , sr , n_frames


def frame_size(sr, size = 0.02): # defult size = 0.02
    return make_power_of_2(int(size*sr))
        

def frequency(frame_size, sr, k):
    return (k*sr)//frame_size


def index_frequency(frame_size, sr, f):
    return (f*frame_size)//sr


def split_into_frames(samples, frame_size):
    frames = []
    for i in range(0, len(samples), frame_size):
        frame = samples[i : i + frame_size]

        if len(frame) == frame_size:
            frames.append(frame)

    return frames


def create_hz_form_frames(frames, sr, frame_size, minF=80, maxF=8000):
    magnitude = []
    low_idx = index_frequency(frame_size, sr, minF)
    high_idx = index_frequency(frame_size, sr, maxF)

    for frame in frames: 
        fft_frame = fft(frame)
        fft_data = volume(fft_frame)
        magnitude.append(fft_data[low_idx : high_idx])

    return magnitude, low_idx


def create_spectrogram(frames, sr, frame_size):
      magnitude, jump = create_hz_form_frames(frames, sr, frame_size)
      return array(magnitude), jump
    

def find_peaks(spectrogram, frame_size, sr, threshold, minF=80, maxF=8000, neighborhood = 3):
    peaks = []

    for t in range(neighborhood, len(spectrogram)-neighborhood):
        for f in range(neighborhood, len(spectrogram[0])-neighborhood):
            value = spectrogram[t, f]
            is_peak = True

            if value < threshold:
                continue
            if not (minF <= frequency(frame_size, sr, f) <= maxF):
                continue

            patch = spectrogram[t - neighborhood: t + neighborhood + 1, f - neighborhood: f + neighborhood + 1,]
            if value == patch.max() and (patch == value).sum() == 1:
                peaks.append((t, f))
            
    return filter_peaks(spectrogram, peaks)    



def filter_peaks(spectrogram, peaks, window_frames=25, max_per_window=5):
    if not peaks:
        return []
 
    filtered = []
    window_peaks = [peaks[0]]
    window_start_t = (peaks[0])[0]
 
    for peak in peaks[1:]:
        if peak[0] > window_start_t + window_frames:
            top = sorted(window_peaks, key=lambda p: spectrogram[p[0], p[1]], reverse=True,)[:max_per_window]
            filtered.extend(top)
            window_start_t = peak[0]
            window_peaks = [peak]
        else:
            window_peaks.append(peak)
 
    if window_peaks:
        top = sorted(window_peaks, key=lambda p: spectrogram[p[0], p[1]], reverse=True,)[:max_per_window]
        filtered.extend(top)
 
    return filtered       


def make_hash(f1, f2, delta_t):
    fingerprint_str = f"{f1},{f2},{delta_t:.4f}"
    return sha1(fingerprint_str.encode()).hexdigest()


def create_threshold(spectrogram):
    return mean(spectrogram) + 2 * std(spectrogram)


def make_fingerprints(peaks, frame_size, sr, jump, max_range=25): # max_range=25 in frames equls to 25*0.02=0.5 sec
    fingerprints = []

    for i, p1 in enumerate(peaks):
        for p2 in peaks[i + 1:]:
            if p2[0] > p1[0] + max_range:
                break
            
            f1 = int(frequency(frame_size, sr, p1[1] + jump))
            f2 = int(frequency(frame_size, sr, p2[1] + jump))
            delta_t = (p2[0] - p1[0]) * frame_size / sr
            
            h = make_hash(f1, f2, delta_t)
            anchor_time = p1[0] * frame_size / sr
            fingerprints.append((h, anchor_time))
            
    return fingerprints


def store_fingerprints(song_name, fingerprints, conn):
    records = [(h, song_name, t) for h, t in fingerprints]
    if records:
        insert_many_fingerprints_copy(conn, records)


def find_matches(query_fingerprints, cur):
    if not query_fingerprints:
        return {}
     
    hash_to_qtimes = defaultdict(list)
    for h, qt in query_fingerprints:
        hash_to_qtimes[h].append(qt)

    hashes = list(hash_to_qtimes.keys())
    db_rows = get_matches_for_hashes(hashes, cur)

    delta_counts = defaultdict(int)
    for db_hash, song_name, db_offset in db_rows:
        for qt in hash_to_qtimes[db_hash]:
            delta = round(db_offset - qt, 1)        
            delta_counts[(song_name, delta)] += 1
    
    if not delta_counts:
        return {}
    
    song_best = {}
    for (song_name, delta), count in delta_counts.items():
        if count > song_best.get(song_name, 0):
            song_best[song_name] = count
 
    return song_best


def find_best_match(counts_matches):
    if not counts_matches:
        return None, 0

    best_match = max(counts_matches, key=counts_matches.get)
    return best_match, counts_matches[best_match]



def pipeline(song_file):
    samples, sr, _ = open_song_wav(song_file)
    frame_sz = frame_size(sr)
    frames_arr = split_into_frames(samples, frame_sz)
    spectrogram, jump = create_spectrogram(frames_arr, sr, frame_sz)
    threshold = create_threshold(spectrogram)
    peaks = find_peaks(spectrogram, frame_sz, sr, threshold)
    return peaks, frame_sz, sr, jump



def analyze_new_song(conn, song_file, song_name):
    print(f"[index] {song_name}")
    peaks, frame_sz, sr, jump = pipeline(song_file)
    print(f"  peaks found: {len(peaks)}")
    fingerprints = make_fingerprints(peaks, frame_sz, sr, jump)
    print(f"  fingerprints: {len(fingerprints)}")
    store_fingerprints(song_name, fingerprints, conn)
    print(f"  stored OK")
 
 
def analyze_query_song(song_file, cur):
    print(f"[query] {song_file}")
    peaks, frame_sz, sr, jump = pipeline(song_file)
    print(f"  peaks found: {len(peaks)}")
    fingerprints = make_fingerprints(peaks, frame_sz, sr, jump)
    print(f"  fingerprints: {len(fingerprints)}")
    scores = find_matches(fingerprints, cur)
    print(f"  scores: {scores}")
    best_song, count = find_best_match(scores)
    return best_song, count