import React, { useState } from 'react';

function Upload() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [errorMessage, setErrorMessage] = useState('');
  const [successMessage, setSuccessMessage] = useState('');

  const handleFileChange = (e) => {
    const file = e.target.files[0];
    setSelectedFile(file);
  };

  const handleUpload = () => {
    if (!selectedFile) {
      setErrorMessage('Please select a CSV file.');
    }
    setSuccessMessage('File uploaded successfully!');
  };


  const formData = new FormData();

  formData.append('csvFile', selectedFile);

  fetch('http://localhost:5000/upload', {
    method: 'POST',
    body: formData,
  })
    .then((response) => {
      if (response.status === 200) {
        setSuccessMessage('File uploaded successfully!');
      } else {
        setErrorMessage('File upload failed. Please try again.');
      }
    })
    .catch((error) => {
      console.error('Error uploading file:', error);
      setErrorMessage('File upload failed. Please try again.');
    });

  return (
    <div className="flex flex-col items-center container mx-auto mt-8">
      <h1 className="text-2xl font-semibold mb-4">Upload CSV File</h1>

      <div className="mb-4">
        <input
          type="file"
          accept=".csv"
          onChange={handleFileChange}
          className="border p-2"
        />
      </div>

      {selectedFile && (
        <div className="mb-4">
          <p className="font-semibold">Selected File:</p>
          <p>{selectedFile.name}</p>
        </div>
      )}

      {errorMessage && <p className="text-red-500 mb-4">{errorMessage}</p>}

      {successMessage && <p className="text-green-500 mb-4">{successMessage}</p>}

      <button onClick={handleUpload} className="text-white py-2 px-4 rounded-md focus:outline-none bg-gradient-to-r from-purple-500 to-pink-500">
        Upload
      </button>

      <div className="mt-8">
        <h2 className="text-xl font-semibold mb-2">Sample CSV Format:</h2>
        <img src="/path-to-sample-image.jpg" alt="Sample CSV Format" />
      </div>
    </div>
  );
}

export default Upload;
