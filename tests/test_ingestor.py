import pytest
import asyncio
from unittest.mock import MagicMock, patch
from src.ingestor import JetstreamIngestor

@pytest.mark.asyncio
async def test_process_message():
    """Test message processing and batching."""
    mock_writer = MagicMock()
    
    with patch('src.ingestor.ParquetWriter', return_value=mock_writer):
        ingestor = JetstreamIngestor('/tmp/test', batch_size=2)
        
        # Process first message
        await ingestor.process_message('{"test": "data1"}')
        assert len(ingestor.messages) == 1
        mock_writer.write_batch.assert_not_called()
        
        # Process second message - should trigger batch write
        await ingestor.process_message('{"test": "data2"}')
        assert len(ingestor.messages) == 0
        mock_writer.write_batch.assert_called_once()

@pytest.mark.asyncio
async def test_flush_messages():
    """Test message flushing."""
    mock_writer = MagicMock()
    
    with patch('src.ingestor.ParquetWriter', return_value=mock_writer):
        ingestor = JetstreamIngestor('/tmp/test')
        ingestor.messages = [{'test': 'data'}]
        
        await ingestor.flush_messages()
        assert len(ingestor.messages) == 0
        mock_writer.write_batch.assert_called_once()
