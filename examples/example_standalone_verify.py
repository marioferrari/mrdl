import hashlib
import os

from mrdl.hasher import verify_file
from mrdl.types import HashSpec
from mrdl.progress import BuiltinProgress

def main():
    # Create a dummy file for demonstration
    filename = "dummy_file.bin"
    print(f"Creating a 10MB dummy file: {filename}")
    
    # 10 MB of random data
    data = os.urandom(10 * 1024 * 1024)
    with open(filename, "wb") as f:
        f.write(data)
    
    # Calculate expected SHA-256
    expected_hash = hashlib.sha256(data).hexdigest()

    # Setup the progress reporter (optional)
    # The progress reporter needs to be started manually for the standalone verify
    progress = BuiltinProgress()
    
    # verify_file uses 1MB chunks by default
    chunk_size = 1024 * 1024 
    total_size = len(data)
    
    progress.start(
        total_bytes=total_size,
        filename=filename,
        chunk_size=chunk_size,
        mode="verify"
    )

    # Verify the file
    try:
        spec = HashSpec.parse(f"sha256:{expected_hash}")
        
        is_valid, computed_hash = verify_file(
            filename=filename,
            hash_spec=spec,
            progress=progress,
            chunk_size=chunk_size
        )
        
        progress.close()
        
        if is_valid:
            print(f"Verification Successful! The file matches the expected checksum.")
            print(f"Expected SHA-256: {expected_hash}")
            print(f"Computed SHA-256: {computed_hash}")
        else:
            print(f"Verification Failed! The file is corrupted.")
            print(f"Expected SHA-256: {expected_hash}")
            print(f"Computed SHA-256: {computed_hash}")
            
    finally:
        # Cleanup dummy file
        if os.path.exists(filename):
            os.unlink(filename)

if __name__ == "__main__":
    main()
