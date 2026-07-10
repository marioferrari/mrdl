import sys
import time

sys.path.insert(0, './src')

from mrdl.progress import BuiltinProgress, MultiProgress

def test_spinner():
    print("Starting progress bar test with total_bytes = 0...")
    progress = BuiltinProgress()
    # start the progress with total size 0 (unknown)
    progress.start(
        total_bytes=0,
        filename="very_important_data_archive.tar.gz",
        chunk_size=1024 * 1024,
    )
    
    # Simulate a slow download that updates completed bytes periodically
    for i in range(30):
        # Update progress with 500 KB every 100ms
        progress.update(500 * 1024)
        time.sleep(0.1)
        
    # Simulate overlay/pause
    progress.set_overlay(" PAUSED ", color="yellow")
    time.sleep(2.0)
    
    # Simulate resume
    progress.set_overlay("")
    for i in range(20):
        progress.update(500 * 1024)
        time.sleep(0.1)
        
    progress.close()
    print("Test finished.")

def test_multispinner():
    print("Starting multi progress bar test with total_bytes = 0...")
    progress_manager = MultiProgress(compact=True)
    bars = []
    
    for i in range(3):
        bar = progress_manager.add_bar()
        bar.start(
            total_bytes=0,
            filename=f"archive_part_{i+1}.tar.gz",
            chunk_size=1024 * 1024,
        )
        bars.append(bar)
    
    for i in range(30):
        for bar in bars:
            bar.update(500 * 1024)
        time.sleep(0.1)
        
    for bar in bars:
        bar.close()
        
    progress_manager.close()
    print("Multi test finished.")

if __name__ == '__main__':
    test_spinner()
    test_multispinner()
