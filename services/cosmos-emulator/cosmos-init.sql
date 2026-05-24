-- Cosmos DB emulator synthetic initialization data
-- Database and container definitions
CREATE DATABASE IF NOT EXISTS factory_ops;
CREATE CONTAINER IF NOT EXISTS shift_data WITH PARTITION KEY /lineId;
CREATE CONTAINER IF NOT EXISTS kpi_data WITH PARTITION KEY /lineId;

-- Shift-related documents
INSERT INTO factory_ops.shift_data VALUES {"id":"shift-1","lineId":"LineA","operator":"Alice","product":"Widget X","shiftDate":"2026-05-22","startTime":"06:00","endTime":"14:00","formsCompleted":["safety-check","quality-inspection"],"status":"completed"};
INSERT INTO factory_ops.shift_data VALUES {"id":"shift-2","lineId":"LineB","operator":"Bob","product":"Widget Y","shiftDate":"2026-05-22","startTime":"14:00","endTime":"22:00","formsCompleted":["handover","packaging-check"],"status":"completed"};
INSERT INTO factory_ops.shift_data VALUES {"id":"shift-3","lineId":"LineC","operator":"Carol","product":"Widget Z","shiftDate":"2026-05-23","startTime":"22:00","endTime":"06:00","formsCompleted":["temperature-log","maintenance-report"],"status":"in-progress"};

-- KPI-related documents
INSERT INTO factory_ops.kpi_data VALUES {"id":"kpi-1","lineId":"LineA","product":"Widget X","goodQty":1180,"wasteQty":20,"efficiency":94.4,"cycleTimeSeconds":42,"timestamp":"2026-05-22T14:00:00Z"};
INSERT INTO factory_ops.kpi_data VALUES {"id":"kpi-2","lineId":"LineB","product":"Widget Y","goodQty":980,"wasteQty":35,"efficiency":89.7,"cycleTimeSeconds":48,"timestamp":"2026-05-22T22:00:00Z"};
INSERT INTO factory_ops.kpi_data VALUES {"id":"kpi-3","lineId":"LineC","product":"Widget Z","goodQty":1025,"wasteQty":15,"efficiency":96.1,"cycleTimeSeconds":39,"timestamp":"2026-05-23T06:00:00Z"};
