const puppeteer = require('puppeteer');
const path = require('path');
const mkdirp = require('mkdirp');
const fs = require('fs');
const uuid = require('uuid');

let wait_for_file_download = async (downloadPath, page) => {
    let wait_for_filename = 100;
    let filename;
    while (!filename || filename.endsWith('.crdownload')) {
        filename = fs.readdirSync(downloadPath)[0];
        await page.waitFor(500);
        wait_for_filename--;
        if (!filename && wait_for_filename <= 0) {
            return 'no-file-here';
        }
    }
    return filename
};

let download_file = async (browser, link) => {
    let fileuuid = uuid();
    const downloadPath = path.resolve(__dirname, 'download', fileuuid);
    mkdirp(downloadPath);

    const page = await browser.newPage();

    await page._client.send('Page.setDownloadBehavior', {behavior: 'allow', downloadPath: downloadPath});
    await page.goto(link).catch(() => {
    });

    let filename = await wait_for_file_download(downloadPath, page);

    if (filename === 'no-file-here') {
        throw "no file to download";
    }

    let newDldPath = downloadPath.replace(`/${fileuuid}`, '');
    fs.renameSync(path.resolve(downloadPath, filename), path.resolve(newDldPath, filename));
    fs.rmdirSync(downloadPath);
    return path.resolve(newDldPath, filename)
};

puppeteer.launch({
    headless: true
}).then(async browser => {

    const page = await browser.newPage();
    page.on('dialog', async dialog => {
        await dialog.dismiss();
    });

    await page.goto('http://pems.dot.ca.gov');


    await page.click('#username');
    await page.keyboard.type('crs.razvan@gmail.com');

    await page.click('#password');
    await page.keyboard.type(')+rL5javak>');

    await Promise.all([
        page.click('input[name="login"]'),
        page.waitForNavigation()
    ]);

    await Promise.all([
        page.click('#std_liquid_left > div > div:nth-child(5) > div.bd > a:nth-child(2)'),
        page.waitForNavigation()
    ]);

    await page.select('#type', 'station_raw');

    await page.select('#district_id', '4');

    await Promise.all([
        page.click('#submit'),
        page.waitForNavigation()
    ]);

    await Promise.all([
        page.click('.dbxWidget > table > tbody > tr:nth-child(5) > td:nth-child(2) > a'),
        await page.waitFor(5000)
    ]);


    await page.waitForXPath('//*[@id="datafiles"]//td/a');
    let link_elements = (await Promise.all((await page.$x('//*[@id="datafiles"]//td/a')).map(handle => handle.getProperty('href')))).map(e => e._remoteObject.value);

    let downloads_file = path.resolve(__dirname, 'download.csv');
    let downloaded_links = [];
    if (fs.existsSync(downloads_file))
        downloaded_links = fs.readFileSync(downloads_file, 'utf8').split('\n');

    link_elements = link_elements.filter(link => downloaded_links.indexOf(link) < 0);
    link_elements = link_elements.filter((link, i) => i <= link_elements.indexOf('http://pems.dot.ca.gov/?download=168881&dnode=Clearinghouse'));

    for (let i = 0; i < link_elements.length; i++) {
        await download_file(browser, link_elements[i]);

        downloaded_links.push(link_elements[i]);
        fs.appendFileSync(downloads_file, `${link_elements[i]}\n`);
    }
    // other actions...
    await browser.close();
});
