import wave 
import numpy as np
from fft import fft,volume
import hashlib
from db import insert_many_fingerprints


def open_song_wav(song_file):
    with wave.open(song_file, "rb") as wf:
        n_channels = wf.getnchannels()
        sample_width = wf.getsampwidth() 
        sr = wf.getframerate()
        n_frames = wf.getnframes() # song time (sec) = n_frames / sr 

        frames = wf.readframes(n_frames)

    samples = np.frombuffer(frames, dtype = np.int16)

    if n_channels == 2:
        samples = samples.reshape(-1, 2)
        samples = samples.mean(axis = 1)

    return samples , sr , n_frames


def frame_size(sr, size = 0.02): # defult size = 0.02
    return int(size*sr)
        

def frequency(frame_size, sr, k):
    return (k*sr)//frame_size


def frames(samples, frame_size):
    frames = []
    for i in range(0, len(samples), frame_size):
        frame = samples[i : i + frame_size]

        if len(frame) == frame_size:
            frames.append(frame)

    return frames


def create_spectrogram(frames):
    spectrogram = []

    for frame in frames: 
        fft_frame = fft(frame)
        magnitude = volume(fft_frame)
        spectrogram.append(magnitude)
    
    return spectrogram


def find_peaks(spectrogram, range_peak):
    peaks = []

    for t in range(3, len(spectrogram)-3):
        for f in range(3, len(spectrogram[0])-3):
            value = spectrogram[t][f]
            is_peak = True

            for i in range(t-3, t+2):
                for j in range(spectrogram[t]-3, spectrogram[t]+2):
                    if i != t and j != f and spectrogram[i][j] >= value:
                        is_peak = False
            
            if is_peak == True:
                peaks.append((t, f))
        
    return peaks    


def threshold(spectrogram):
    return np.mean(spectrogram) + np.std(spectrogram)


def filering_peaks(spectrogram, peaks, frame_size, sr, thresshold=0.4, minF=50, maxF=5000): 
    filtered_peaks = [
        p for p in peaks 
        if minF <= frequency(frame_size, sr, p[1]) <= maxF and spectrogram[p[0], p[1]] >= threshold
    ]
    return filtered_peaks

def process_peaks(song_name, peaks, frame_size, sr, max_range=25): # max_range=25 in frames equls to 25*0.02=0.5 sec
    all_hashes = []

    for i, p1 in enumerate(peaks):
        for p2 in peaks[i + 1:]:
            if p2[0] > p1[0] + max_range:
                break
            
            f1 = int(frequency(frame_size, sr, p1[1]))
            f2 = int(frequency(frame_size, sr, p2[1]))
            
            fingerprint = (f1, f2, (p2[0]-p1[0])*frame_size/sr)
            fingerprint_str = str(fingerprint)
            h = hashlib.sha1(fingerprint_str.encode()).hexdigest()

            anchor_time = p1[0] * frame_size / sr

            all_hashes.append((h, song_name, anchor_time))

    return all_hashes


def store_hashes(all_hashes, cur, conn):
    if len(all_hashes) > 0:
        insert_many_fingerprints(cur, all_hashes)
        conn.commit()


def process_query_peaks(peaks, frame_size, sr, max_range=25): # max_range=25 in frames equls to 25*0.02=0.5 sec
    query_hashes = []

    for i, p1 in enumerate(peaks):
        for p2 in peaks[i + 1:]:
            if p2[0] > p1[0] + max_range:
                break
            
            f1 = int(frequency(frame_size, sr, p1[1]))
            f2 = int(frequency(frame_size, sr, p2[1]))
            
            fingerprint = (f1, f2, (p2[0]-p1[0])*frame_size/sr)
            fingerprint_str = str(fingerprint)
            h = hashlib.sha1(fingerprint_str.encode()).hexdigest()
            query_hashes.append(h)

    return query_hashes



def find_matches(hashes, cur):
    counts_matches = {}

    for h in hashes:
        cur.execute("SELECT song_id, offset_time FROM fingerprints WHERE hash = %s", (h,))
        results = cur.fetchall()

        for song_id in results:
            counts_matches[song_id] = counts_matches.get(song_id, 0) + 1

    return counts_matches



def analyze_new_song(cur, conn, song_file):
    samples, sr, n_frames = open_song_wav(song_file)
    frame_sz = frame_size(sr)
    frames_list = frames(samples, frame_sz)
    spectrogram = create_spectrogram(frames_list)
    peaks = find_peaks(spectrogram, range_peak = 10)
    threshold = threshold(spectrogram)
    filtered_peaks = filering_peaks(spectrogram, peaks, frame_sz, sr, threshold)
    all_hashes = process_peaks(song_file, filtered_peaks, frame_sz, sr)
    store_hashes(all_hashes, cur, conn)


def analyze_query_song(song_file):
    samples, sr, n_frames = open_song_wav(song_file)
    frame_sz = frame_size(sr)
    frames_list = frames(samples, frame_sz)
    spectrogram = create_spectrogram(frames_list)
    peaks = find_peaks(spectrogram, range_peak = 10)
    threshold = threshold(spectrogram)
    filtered_peaks = filering_peaks(spectrogram, peaks, frame_sz, sr, threshold)
    query_hashes = process_query_peaks(filtered_peaks, frame_sz, sr)

    return filtered_peaks