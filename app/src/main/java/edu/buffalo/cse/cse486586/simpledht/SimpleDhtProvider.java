package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.*;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static java.lang.System.out;

public class SimpleDhtProvider extends ContentProvider {

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static String node_id;
    static String myPort;
    static String portStr;
    static String predecessor = "";
    static String successor = "";
    static String pred_port = null;
    static String succ_port = null;
    static final int SERVER_PORT = 10000;
    static boolean insert_complete = false;
    static boolean query_complete = false;
    static String query_result = "";
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    /**
     * buildUri() demonstrates how to build a URI for a ContentProvider.
     *
     * @param scheme
     * @param authority
     * @return the URI
     */
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));


        try{
            node_id = this.genHash(portStr);
        }
        catch(NoSuchAlgorithmException e)
        {
            Log.e(TAG, e.toString());
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerJoinTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.htmlmyPort
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            Log.getStackTraceString(e);
        }


       new ChordJoin().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);

        return false;
    }


    private class ServerJoinTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {

            ServerSocket serverSocket = serverSockets[0];
            Socket clientSocket = null;
            BufferedReader in = null;
            PrintWriter out = null;
            String inputString;

            while (true)
            {

                try {
                    clientSocket = serverSocket.accept();
                    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    out = new PrintWriter(clientSocket.getOutputStream(), true);

                    if ((inputString = in.readLine()) != null) {
                        String[] split_input = inputString.split(",");
                        if (split_input[0].equals("join")) {
                            if (predecessor.equals("")  && successor.equals("")) {
                                Log.v("server", "comes here");
                                out.println(myPort + "," + myPort);
                            } else {
                                if (split_input[1].compareTo(genHash(pred_port)) > 0 && split_input[1].compareTo(node_id) < 0) {
                                    out.println(predecessor + "," + myPort);
                                } else if (genHash(pred_port).compareTo(node_id) > 0 && (split_input[1].compareTo(genHash(pred_port)) > 0 || split_input[1].compareTo(node_id) < 0)) {
                                    out.println(predecessor + "," + myPort);
                                } else {

                                    String res = this.successor_lookup(split_input[1]);
                                    out.println(res);
                                }
                            }
                        } else if (split_input[0].equals("update")) {
                            if (split_input[1].equals("predecessor")) {
                                predecessor = split_input[2];
                                pred_port = String.valueOf(Integer.parseInt(predecessor) / 2);
                                Log.v("update predecessor>>>", "predecessor updated");
                                Log.v("Predecessor, Successor", predecessor + "," + successor);
                            } else {
                                successor = split_input[2];
                                succ_port = String.valueOf(Integer.parseInt(successor) / 2);
                                Log.v("update successor>>>", "successor updated");
                                Log.v("Predecessor, Successor", predecessor + "," + successor);
                            }
                        } else if (split_input[0].equals("insert")) {
                            Log.v("server", "received at successor");
                            Log.v("input string", inputString);
                            Log.v("pred port", pred_port);
                            String filename = genHash(split_input[1]);
                            if ((filename.compareTo(genHash(pred_port)) > 0 && filename.compareTo(node_id) < 0) ||
                                    (genHash(pred_port).compareTo(node_id) > 0 && ((filename.compareTo(genHash(pred_port)) > 0 || filename.compareTo(node_id) < 0))))
                            {
                                ContentValues cv = new ContentValues();
                                cv.put(KEY_FIELD, split_input[1]);
                                cv.put(VALUE_FIELD, split_input[2]);
                                try {
                                    insert(mUri, cv);
                                } catch (Exception ex) {
                                    Log.e(TAG, ex.toString());

                                }
                                out.println("success");
                            }
                            else
                            {
                                String res = this.successor_insert(split_input[1], split_input[2]);
                                out.println(res);
                            }

                        } else if (split_input[0].equals("query")) {
                            int keyIndex;
                            int valueIndex;
                            String return_string = "";
                            String returnKey = "";
                            String returnValue = "";
                            String filename = genHash(split_input[1]);
                            if ((filename.compareTo(genHash(pred_port)) > 0 && filename.compareTo(node_id) < 0) || (split_input[1].equals("*")) ||
                                    (genHash(pred_port).compareTo(node_id) > 0 && ((filename.compareTo(genHash(pred_port)) > 0 || filename.compareTo(node_id) < 0))))
                            {
                                try {

                                    if(split_input[1].equals("*"))
                                    {
                                        Cursor resultCursor = query(mUri, null, "@", null, null);

                                        keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                                        valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
//                                        resultCursor.moveToFirst();
                                        for(resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext())
                                        {
                                            returnKey = resultCursor.getString(keyIndex);
                                            returnValue = resultCursor.getString(valueIndex);
                                            return_string = return_string + returnKey + "," + returnValue + ";";
                                            Log.v("val = ", return_string);
                                            resultCursor.close();

                                        }
                                        if(successor.equals(split_input[2]))
                                        {
                                            Log.v("end: aggregated value:", return_string);
                                            out.println(return_string);
                                        }
                                        else
                                        {
                                            String res = this.successor_query(split_input[1], split_input[2]);
                                            Log.v("aggregated value:", return_string+res);
                                            out.println(return_string+res);
                                        }
                                    }
                                    else {
                                        Cursor resultCursor = query(mUri, null, split_input[1].trim(), null, null);

                                        keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                                        valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

                                        resultCursor.moveToFirst();

                                        returnKey = resultCursor.getString(keyIndex);
                                        returnValue = resultCursor.getString(valueIndex);
                                        out.println((returnKey+","+returnValue));
                                        resultCursor.close();
                                    }

                                }
                                catch (Exception ex) {
                                    Log.e(TAG, ex.toString());
                                }

                            }
                            else
                            {
                                String res = this.successor_query(split_input[1], split_input[2]);
                                out.println(res);
                            }

                        } else if (split_input[0].equals("delete")) {

                            String filename = genHash(split_input[1]);
                            if ((filename.compareTo(genHash(pred_port)) > 0 && filename.compareTo(node_id) < 0) || (split_input[1].equals("*")) ||
                                    (genHash(pred_port).compareTo(node_id) > 0 && ((filename.compareTo(genHash(pred_port)) > 0 || filename.compareTo(node_id) < 0))))
                            {
                                try {

                                    if(split_input[1].equals("*"))
                                    {
                                        if(delete(mUri, "@", null) == 1)
                                            Log.v(myPort, "deleted all my files");
                                        else
                                            Log.v(myPort, "Could not delete all my files");

                                        if(successor.equals(split_input[2]))
                                        {
                                            out.println("success");
                                        }
                                        else
                                        {
                                            String res = this.successor_delete(split_input[1], split_input[2]);
                                            out.println(res);
                                        }
                                    }
                                    else {
                                        delete(mUri, split_input[1],null);
                                        out.println("success");
                                    }

                                }
                                catch (Exception ex) {
                                    Log.e(TAG, ex.toString());
                                }

                            }
                            else
                            {
                                String res = this.successor_delete(split_input[1], split_input[2]);
                                out.println(res);
                            }

                        }

                        if (clientSocket.isClosed()) {
                            break;
                        }
                    }
                } catch (UnknownHostException e) {
                    Log.e(TAG, e.toString());
                } catch (IOException e) {
                    Log.e(TAG, e.toString());
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, e.toString());
                }
            }
            return null;
        }

        String successor_lookup(String incoming_node_id)
        {
            PrintWriter out;
            BufferedReader in;
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out.println("join,"+ incoming_node_id);
                String res;
                if((res = in.readLine()) != null)
                {
                    return res;
                }
            }

            catch (SocketException e) {
                Log.e(TAG, e.toString());
            }

            catch (UnknownHostException e) {
                Log.e(TAG, e.toString());
            }
            catch (IOException e) {
                Log.e(TAG, e.toString());
            }
            return null;
        }

        String successor_insert(String key, String value)
        {
            PrintWriter out = null;
            BufferedReader in = null;
            Socket socket = null;

            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out.println("insert"+"," + key + "," + value);
                String input_string;
                if((input_string = in.readLine()) != null) {
                    return input_string;
                }


            } catch (SocketException e) {
                Log.e(TAG, e.toString());
            } catch (UnknownHostException e) {
                Log.e(TAG, e.toString());
            } catch (IOException e) {
                Log.e(TAG, e.toString());
            }
            return null;
        }
        String successor_query(String selection, String port)
        {
            PrintWriter out = null;
            BufferedReader in = null;
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out.println("query"+"," + selection+","+port);
                String input_string;
                if((input_string = in.readLine()) != null) {

                    return input_string;
                }
            } catch (SocketException e) {
                Log.e(TAG, e.toString());
            } catch (UnknownHostException e) {
                Log.e(TAG, e.toString());
            } catch (IOException e) {
                Log.e(TAG, e.toString());
            }
            return null;
        }

        String successor_delete(String selection, String port)
        {
            PrintWriter out = null;
            BufferedReader in = null;
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out.println("delete" + "," + selection+","+ port);
                String input_string;
                if((input_string = in.readLine()) != null) {

                    return input_string;
                }
            } catch (SocketException e) {
                Log.e(TAG, e.toString());
            } catch (UnknownHostException e) {
                Log.e(TAG, e.toString());
            } catch (IOException e) {
                Log.e(TAG, e.toString());
            }
            return null;
        }

    }

    private class ChordJoin extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {


            PrintWriter out =null;
            BufferedReader in = null;
            Socket socket = null;

                try {
                        if(myPort.equals("11108") && successor.equals("") && predecessor.equals(""))
                        {
                            Log.v("Chord Join", "comes here");
                            successor = predecessor = "";
                        }
                        else {

                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                            out = new PrintWriter(socket.getOutputStream(), true);
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            out.println("join," + node_id);
                            String input_string, split_input[];
                            if ((input_string = in.readLine()) != null) {
                                split_input = input_string.split(",");
                                predecessor = split_input[0];
                                successor = split_input[1];
                                pred_port = String.valueOf(Integer.parseInt(predecessor) / 2);
                                succ_port = String.valueOf(Integer.parseInt(successor) / 2);
                                Log.v("Predecessor, Successor", predecessor+","+successor);
                            }
                        }
                        if(!successor.equals("") && !predecessor.equals(""))
                        {
                            //Updating the successor's predecessor
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                            out = new PrintWriter(socket.getOutputStream(), true);
                            out.println("update," + "predecessor" + "," + myPort);

                            //Updating the predecessor's successor
                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(predecessor));
                            out = new PrintWriter(socket.getOutputStream(), true);
                            out.println("update," + "successor" + "," + myPort);
                        }

                }
                catch (SocketException e) {
                    predecessor = "";
                    successor = "";
                    Log.e(TAG, e.toString());
                }

                catch (UnknownHostException e) {
                    Log.e(TAG, e.toString());
                }
                catch (IOException e) {
                    Log.e(TAG, e.toString());
                }

            return null;

        }
    }


    private String passToSuccessor(String op, String val1, String val2) {


            PrintWriter out = null;
            BufferedReader in = null;
            Socket socket = null;

            try {
                    Log.v("passtosuccessor",val1);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                    out = new PrintWriter(socket.getOutputStream(), true);
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    out.println(op+"," + val1 + "," + val2);
                    String input_string;
                    if((input_string = in.readLine()) != null) {
                        Log.v("received from succ:", input_string);
                        return input_string;
                    }

            }
            catch (SocketException e) {
                Log.e(TAG, e.toString());
            } catch (UnknownHostException e) {
                Log.e(TAG, e.toString());
            } catch (IOException e) {
                Log.e(TAG, e.toString());
            }
            return null;

    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {


        String[] columnNames = {KEY_FIELD, VALUE_FIELD};
        MatrixCursor cursor = null;
        cursor = new MatrixCursor(columnNames);
        FileInputStream fis = null;
        String hashedfilename;

        try {

            if(selection.equals("@"))
            {
                for (String file: getContext().fileList()) {
                    Log.v("file--->", file);
                    fis = getContext().openFileInput(file);
                    InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
                    BufferedReader reader = new BufferedReader(inputStreamReader);
                    String line = reader.readLine();
                    String[] columnValues = {file, line};

                    cursor.addRow(columnValues);
//                    cursor.close();
                    reader.close();
                    inputStreamReader.close();
                }
                Log.v("count of all local keys", String.valueOf(cursor.getCount()));

            }
            else if(selection.equals("*"))
            {
                Log.v("selector:", selection);
                for (String file: getContext().fileList()) {
                    Log.v("file--->", file);
                    fis = getContext().openFileInput(file);
                    InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
                    BufferedReader reader = new BufferedReader(inputStreamReader);
                    String line = reader.readLine();
                    String[] columnValues = {file, line};

                    cursor.addRow(columnValues);
                    reader.close();
                    inputStreamReader.close();
                }

                if(!predecessor.equals("") && !successor.equals(""))
                {
                    String result = passToSuccessor("query","*", myPort);
                    Log.v("final received value", result);

                    for(String row: result.trim().split(";"))
                    {
                        Log.v("row:", row);
                        String[] split = row.split(",");
                        if(split.length == 2)
                            cursor.addRow(split);
                        else
                        {
                            Log.v("bad split", split.toString());
                        }
                    }
                }
                Log.v("final count", String.valueOf(cursor.getCount()));
//                cursor.close();

            }
            else
            {
                hashedfilename = genHash(selection.trim());
                if(predecessor.equals("") && successor.equals(""))
                {
                    Log.v("query", "query string = "+ selection);
                    fis = getContext().openFileInput(selection);
                    InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
                    BufferedReader reader = new BufferedReader(inputStreamReader);
                    String line = reader.readLine();
                    Log.v("value of line", line);
                    String[] columnValues = {selection, line};

                    cursor.addRow(columnValues);
                    reader.close();
                    inputStreamReader.close();
                }
                else if((hashedfilename.compareTo(genHash(pred_port)) >0 && hashedfilename.compareTo(node_id) < 0) ||
                        (genHash(pred_port).compareTo(node_id) > 0 && (hashedfilename.compareTo(genHash(pred_port)) > 0 || hashedfilename.compareTo(node_id)  < 0)) )
                {
                    fis = getContext().openFileInput(selection);
                    InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
                    BufferedReader reader = new BufferedReader(inputStreamReader);
                    String line = reader.readLine();
                    String[] columnValues = {selection, line};
                    cursor.addRow(columnValues);
                    reader.close();
                    inputStreamReader.close();
                }
                else
                {
                    String result = "";
                    result = passToSuccessor("query", selection, myPort);
                    Log.v("result of query", result);
                    String[] columnValues = {selection, result.split(",")[1]};
                    cursor.addRow(columnValues);

                }
//                cursor.close();
            }
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.e(TAG, e.toString());
        }

        catch (IOException e)
        {
            Log.e("error content provider", e.toString());
            String[] columnValues = {selection, "N/A"};
            cursor.addRow(columnValues);
        }
    cursor.close();
    return cursor;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        String hashedfilename;
        Log.v("delete", "inside delete function");
        try {

            if(selection.equals("@"))
            {
                for (String file: getContext().fileList()) {
                    Log.v("delete", "listing files");

                    if(getContext().deleteFile(file)) {
                        Log.v("delete", "file deleted successfully");
                    }
                    else {
                        Log.v("delete", "File deletion unsuccessful");
                    }

                }

            }
            else if(selection.equals("*"))
            {
                Log.v("deletion:", "delete everything");
                for (String file: getContext().fileList()) {
                    if(getContext().deleteFile(file)) {
                        Log.v("delete", "file deleted successfully");
                    }
                    else {
                        Log.v("delete", "File deletion unsuccessful");
                    }
                }

                if(!predecessor.equals("") && !successor.equals(""))
                {
                    String result = passToSuccessor("delete","*", myPort);
                    Log.v("deletion result", result);

                }
                Log.v("final count", String.valueOf(getContext().fileList().length));

            }
            else {
                hashedfilename = genHash(selection);
                if (predecessor.equals("") && successor.equals("")) {
                    if (getContext().deleteFile(selection)) {
                        Log.v("delete", selection+" deleted successfully");
                    } else {
                        Log.v("delete", selection+ " deletion unsuccessful");
                    }
                }
                else if ((hashedfilename.compareTo(genHash(pred_port)) > 0 && hashedfilename.compareTo(node_id) < 0) ||
                        (genHash(pred_port).compareTo(node_id) > 0 && (hashedfilename.compareTo(genHash(pred_port)) > 0 || hashedfilename.compareTo(node_id) < 0))) {
                    if (getContext().deleteFile(selection)) {
                        Log.v("delete", selection+" deleted successfully");
                    } else {
                        Log.v("delete", selection+ " deletion unsuccessful");
                    }
                } else {
                    String result = "";
                    result = passToSuccessor("delete", selection, myPort);
                    Log.v("result of deletion", result);

                }
            }


        }
        catch (NoSuchAlgorithmException e)
        {
            Log.v("exception caught", "Exception caught at delete");
            Log.e(TAG, e.toString());
        }

        return 1;

    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        Iterator iter = values.keySet().iterator();
        String key = "", value = "", hashedfilename="";
        OutputStream outputStream;
        Log.v("input for insert", values.toString());
        while(iter.hasNext())
        {
            String col = (String) iter.next();

            if(col.equals(VALUE_FIELD))
            {
                value = values.getAsString(col)+"\n";
            }

            else
            {
                key = values.getAsString(col);
                try {
                    Log.v("key", key);
                    hashedfilename = genHash(key);
                    Log.v("Reached here", predecessor+ ","+successor);
                    if(predecessor.equals("") && successor.equals(""))
                    {
                        Log.v("entered the if", "enters the if");
                        outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                        outputStream.write(value.getBytes());
                        outputStream.close();
                        Log.v("insert", "insert successful");
                    }

                    else if((hashedfilename.compareTo(genHash(pred_port)) >0 && hashedfilename.compareTo(node_id) < 0) ||
                            (genHash(pred_port).compareTo(node_id) > 0 && (hashedfilename.compareTo(genHash(pred_port)) > 0 || hashedfilename.compareTo(node_id)  < 0)) )
                    {

                        outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                        outputStream.write(value.getBytes());
                        outputStream.close();
                        Log.v("insert", "insert successful");
                    }
                    else
                    {
                        passToSuccessor("insert", key, value);

                    }

                }
                catch (NoSuchAlgorithmException e)
                {
                    Log.e(TAG, e.toString());
                }
                catch (IOException e) {
                    Log.e("GroupMessengerProvider", "File write failed");
                }
            }
        }

        return uri;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
