import React, { useState } from 'react';
import './App.css'; // Import your CSS file

function App() {
  const [inputSeq, setInputSeq] = useState('');
  const [transactions, setTransactions] = useState([]);
  const [result, setResult] = useState('');
  const [error, setError] = useState('');
  const [algorithm, setAlgorithm] = useState('algorithm1'); // Add this line


  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const response = await fetch(`http://127.0.0.1:5000/${algorithm}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ input_seq: inputSeq })
      });

      const data = await response.json();
      if (data.result) {
        setResult(data.result);
        setTransactions(parseTransactionData(data.result));
        setError('');
      } else {
        setError('Error: ' + data.error);
        setResult('');
        setTransactions([]);
      }
    } catch (error) {
      setError('Error: ' + error.message);
      setResult('');
      setTransactions([]);
    }
  };

  const handleAlgorithmChange = (event) => {
    setAlgorithm(event.target.value);
  };

  const parseTransactionData = (resultString) => {
    const transactions = [];
    const regex = /([RSUWCXL]+)(\d+)(\(\w*\))?;?/g;
    let match;
    let row = 1;
  
    while ((match = regex.exec(resultString)) !== null) {
      const operation = match[1];
      const id = parseInt(match[2]);
      const table = match[3] ? match[3].replace(/[\(\)]/g, '') : '';
  
      transactions.push({
        row,
        column: id,
        value: `${operation}${id}${table ? `(${table})` : ''}`
      });
  
      row++;
    }
  
    return transactions;
  };
  const generateTableData = () => {
    const transactionData = parseTransactionData(result);
    const maxColumn = Math.max(...transactionData.map(data => data.column));
  
    const rows = [];
    transactionData.forEach(data => {
      if (!rows[data.row]) {
        rows[data.row] = Array(maxColumn).fill('');
      }
      rows[data.row][data.column - 1] = data.value;
    });
  
    return (
      <table>
        <thead>
          <tr>
            {Array.from({ length: maxColumn }, (_, i) => <th key={i}>{`T${i + 1}`}</th>)}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index + 1}>
              {row.map((data, columnIndex) => (
                <td key={`${index}-${columnIndex + 1}`}>
                  {data ? data : ''}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    );
  };
  
  return (
    <div className="App">
      <div>
        <h1>Concurrency Control Protocol</h1>
      </div>
      <div className='searchbar'>
        <form onSubmit={handleSubmit}>
          <input type='text' placeholder='Enter sequence...' value={inputSeq} onChange={(e) => setInputSeq(e.target.value)} />
          <button type='submit'>Process</button>
        </form>
      </div>
      <div>
        <input type="radio" id="twophase" name="twophase" value="twophase" checked={algorithm === 'twophase'} onChange={handleAlgorithmChange} />
        <label for="algorithm1">Two Phase Locking</label><br/>
        <input type="radio" id="occ" name="occ" value="occ" checked={algorithm === 'occ'} onChange={handleAlgorithmChange} />
        <label for="algorithm2">Optimistic Concurrency Control</label><br/>
      </div>
        <div className='column'>
          <div className='table-container'>
            {generateTableData()}
            {error && <p className="error">Error: {error}</p>}
          </div>
        </div>
        <div className='finalschedule'>
          <p><b>Final Schedule:</b> {result}</p>
        </div>
      </div>
  );
}

export default App;
