package db.ui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.sql.Connection;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ProgressMonitor;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.table.DefaultTableModel;

import db.ConnectionUtils;

public class DBLogShrinkWindow extends JFrame {
	
	static final String PASSWORD = "PASSWORD";
	static final String USERNAME = "USERNAME";
	static final String DBSERVER = "DBSERVER";
	
	private static final int WIDTH = 500;
	private static final int DISPLAY_PANEL_HEIGHT = 450;
	private static final int ENTRY_PANEL_HEIGHT = 100;
	JTextField dbTextField;
	JTextField userTextField;
	JTextField passwordTextField;
	private JButton shrinkButton;
	private JButton refreshButton;
	private JTable table;
	private JScrollPane scrollPane;
	JProgressBar progressBar;
	ProgressMonitor progressMonitor;
	
	private MyTableModel tableModel;
	DBLogShrinkController controller;

	public DBLogShrinkWindow() {
		controller = new DBLogShrinkController(this);
		dbTextField = new JTextField(controller.getDefaultDBServerURL());
		userTextField = new JTextField(controller.getDefaultUsername());
		passwordTextField = new JTextField(controller.getDefaultPassword());
		refreshButton = new JButton("Refresh");
		shrinkButton = new JButton("Shrink");
		tableModel = new MyTableModel();
		createAndShowGUI();
		
		refreshButton.addActionListener(e -> {
			progressBar.setValue(0);
			SwingUtilities.invokeLater(() -> {
				controller.fetch(this::onDataFetchComplete); 
			});
			savePreferences();
		});
		
		shrinkButton.addActionListener(e -> {
			
			SwingUtilities.invokeLater(() -> { 
				controller.shrink(() -> {
					JOptionPane.showMessageDialog(this, "Shrinking Complete. Refreshing db table sizes");
					controller.fetch(this::onDataFetchComplete);
				});
			});
			savePreferences();
		});
	}
	
	public void onDataFetchComplete(Object[][] data) {
		refreshTable(data);
		showTxLogSize(); 
	}
	
	public void onInitialDataFetchComplete(Object[][] data) {
		initTable(data);
		showTxLogSize(); 
	}
	
	public void onShrinkComplete(Object[][] data) {
		initTable(data);
		showTxLogSize(); 
	}
	
	private void savePreferences() {
		Preferences preferences = Preferences.userNodeForPackage(this.getClass());
		preferences.put(DBSERVER, dbTextField.getText().trim());
		preferences.put(USERNAME, userTextField.getText().trim());
		preferences.put(PASSWORD, passwordTextField.getText().trim());
		try {
			preferences.flush();
		} catch (BackingStoreException e) {
			e.printStackTrace();
		}
	}

	private final void createAndShowGUI() {
		
		BorderLayout mainLayout = new BorderLayout();
		GridLayout dbEntryLayout = new GridLayout(4, 2);
		FlowLayout displayLayout = new FlowLayout();
		setLayout(mainLayout);
        progressBar = new JProgressBar(0, 100);
        progressBar.setValue(0);
        progressBar.setStringPainted(true);
		
		JPanel dbEntryPanel = new JPanel();
		dbEntryPanel.setLayout(dbEntryLayout);
		dbEntryPanel.add(new JLabel("DB Server"));
		dbEntryPanel.add(dbTextField);
		dbEntryPanel.add(new JLabel("Username"));
		dbEntryPanel.add(userTextField);
		dbEntryPanel.add(new JLabel("Password"));
		dbEntryPanel.add(passwordTextField);		
		dbEntryPanel.add(refreshButton);
		dbEntryPanel.add(shrinkButton);
		add(dbEntryPanel, BorderLayout.NORTH);
		
		dbEntryPanel.setPreferredSize(new Dimension(WIDTH, ENTRY_PANEL_HEIGHT));
		
		JPanel displayPanel = new JPanel();
		displayPanel.setLayout(displayLayout);
		table = createTable();
		scrollPane = new JScrollPane(table);
		displayPanel.add(scrollPane);
		add(displayPanel, BorderLayout.CENTER);
		
		add(progressBar, BorderLayout.SOUTH);
		displayPanel.setPreferredSize(new Dimension(WIDTH, DISPLAY_PANEL_HEIGHT));
		
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		pack();
		SwingUtilities.invokeLater(() -> {			
			controller.fetch(this::onInitialDataFetchComplete);
		});
	}

	public void showTxLogSize() {
		JOptionPane.showMessageDialog(this, "Total transaction log size: " + NumberFormat.getInstance().format(tableModel.txLogSize()) + " MB");
	}
	
	JTable createTable() {
		
		return new JTable(tableModel);
	}
	
	public static void main(String[] args) {
		SwingUtilities.invokeLater(() -> {
			showWindow();
		});
	}

	public static void showWindow() {
		DBLogShrinkWindow ui = new DBLogShrinkWindow();
		ui.setVisible(true);
		ui.setSize(new Dimension(WIDTH, (int)((ENTRY_PANEL_HEIGHT + DISPLAY_PANEL_HEIGHT) * 1.1)));
		ui.setTitle("DB Tx Log Shrink Utility");
	}
	
	void initTable(Object[][] data) {
		setupTableColumns();
		setupTableData(data);
		setupTableRows(data);
	}

	void refreshTable(Object[][] data) {
		removeAllRows();
		setupTableData(data);
		setupTableRows(data);
	}
	
	public void setupTableColumns() {
		tableModel.setColumnIdentifiers(new Object[]{"Database", "Size (MB)", "Log File name"});
	}	
	
	public void setupTableData(Object[][] data) {
		tableModel.setData(data);
	}

	public void setupTableRows(Object[][] data) {
		for(int i=0; i < data.length; i++) {
			tableModel.addRow(data[i]);
		}
	}
	
	void removeAllRows() {
		int rowCount = tableModel.getRowCount();
		for(int i = rowCount - 1; i >= 0; i--) {
		    tableModel.removeRow(i);
		}
	}
}

class MyTableModel extends DefaultTableModel {
	
	private Object[][] data;
	
	double txLogSize() {
		double size = 0.0;
		for(int i=0; i < data.length; i++) {
			size += (Double)data[i][1];
		}
		return size;
	}
	
	public synchronized void setData(Object[][] data) {
		this.data = data;
	}
	
	public synchronized Object[][] getData() {
		return data;
	}
}

class DBLogShrinkController {
	
	private final int PARALLEL_THREADS = 8; 
	private final ExecutorService executorService = Executors.newFixedThreadPool(PARALLEL_THREADS); 
	
	private final DBLogShrinkWindow view;
	private int totalCount = 0;
	private AtomicInteger progressCount = new AtomicInteger(0);
	
	public DBLogShrinkController(DBLogShrinkWindow view) {
		this.view = view;
		view.controller = this;
	}
	
	public void fetch(Consumer<Object[][]> consumer) {
		
		new Thread(() -> {
			List<Connection> connections = getConnections(PARALLEL_THREADS);
			Connection connection = connections.get(0);
			List<String> databases = ConnectionUtils.getDatabases(connection);
			int databasesCount = databases.size();
			Object[][] data = new Object[databasesCount][3];
			Future[] futures = new Future[databasesCount];
			
			setupProgressBar(databasesCount);
		
			for(int i=0; i < databasesCount; i++) {
				final String db = databases.get(i);
				data[i][0] = db;
				Connection conn = connections.get(i % PARALLEL_THREADS);
				final int j = i;
				futures[i] = executorService.submit(() -> {
					Object[] txDetails = ConnectionUtils.getTxLogFileDetails(conn, db);
					if(txDetails != null) {
						data[j][1] = txDetails[0];
						data[j][2] = txDetails[1];
					} else {
						data[j][1] = "NA";
						data[j][2] = "NA";
					}
					
					try {
						TimeUnit.MILLISECONDS.sleep(250);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					updateProgressBar();
				});
			}
			
			waitTillComplete(futures);
			updateProgressBar(100);
			
			consumer.accept(data);
		}).start();		
	}

	public void waitTillComplete(Future[] futures) {
		for(Future<?> future : futures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
	}	
	
	public void shrink(Runnable r) {
		new Thread( () -> {
			List<Connection> connections = getConnections(PARALLEL_THREADS);
			Connection connection = connections.get(0);
			List<String> databases = ConnectionUtils.getDatabases(connection);
			int databasesCount = databases.size();
			setupProgressBar(databasesCount);
			Future<?>[] futures = new Future<?>[databases.size()];
			for(int i=0; i < databases.size(); i++) {
				String db = databases.get(i);
				Connection conn = connections.get(i % PARALLEL_THREADS);
				futures[i] = executorService.submit(() -> {
					ConnectionUtils.shrinkTransaction(conn, db);
					updateProgressBar();
					view.repaint();
				});
			}
			
			waitTillComplete(futures);
			updateProgressBar(100);
			
			r.run();	
		}).start();
	}	
	
	private synchronized void  setupProgressBar(int totalCount) {
		this.totalCount = totalCount;
		this.progressCount.set(0);
	}

	public void updateProgressBar() {
		int i = Math.max(this.progressCount.incrementAndGet(), 1);
		updateProgressBar(i * 100/ totalCount);
	}
	
	public void updateProgressBar(double percentage) {
		try {
			SwingUtilities.invokeLater(() ->  {
//				System.out.printf("[%s] i = %d\tPercentage = %d%n", Thread.currentThread().getName(), i, (i*100)/totalCount );
				
				view.progressBar.setValue((int)percentage);
				view.progressBar.repaint();
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	private List<Connection> getConnections(int threads) {
		List<Connection> connections = new ArrayList<>(threads);
		for(int i=0; i < threads; i++) {
			String db = view.dbTextField.getText().trim();
			String user = view.userTextField.getText().trim();
			String password = view.passwordTextField.getText().trim();
			String key = ConnectionUtils.connectionPoolKey(db, user, password) + "-" + i;
			connections.add(ConnectionUtils.connection(db, user, password, key));
		}
		return connections;
	}
	
	public Connection getConnection() {
		return ConnectionUtils.connection(view.dbTextField.getText().trim(), view.userTextField.getText().trim(), view.passwordTextField.getText().trim());
	}
	
	public String getDefaultDBServerURL() {
		Preferences preferences = Preferences.userNodeForPackage(this.getClass());
		return preferences.get(DBLogShrinkWindow.DBSERVER, getDefaultHostName() + "\\" + getDefaultDBServer());
	}
	
	public String getDefaultHostName() {
		String hostname = System.getenv("COMPUTERNAME");
		return hostname != null ? hostname : "localhost";
	}
	
	public String getDefaultDBServer() {
		return "G3SQL01";
	}
	
	public String getDefaultUsername() {
		Preferences preferences = Preferences.userNodeForPackage(this.getClass());
		return preferences.get(DBLogShrinkWindow.USERNAME, "sa");
	}
	
	public String getDefaultPassword() {
		Preferences preferences = Preferences.userNodeForPackage(this.getClass());
		return preferences.get(DBLogShrinkWindow.PASSWORD, "IDeaS123");
	}
}