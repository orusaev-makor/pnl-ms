const DBService = require('../../services/db.service');

// Data for 'daily' view
async function queryDailyView() {
        let filter = {
                requestedPnl: 'net',
                chosenDate: '2020-10-20',  // YYYY-MM-DD
                chosenDate2: 'CURDATE()',  // YYYY-MM-DD
                clientId: null,
                employeeId: null
        };
        // TODO: DONE
        var queryProductPerCompanyMonthly2020 = `
        SELECT product_name, company, 
	(CASE WHEN trading_createAt >= DATE(CURDATE()) THEN SUM(net) END) 'todayPnl',
        (CASE WHEN WEEK(trading_createAt) = WEEK(CURDATE()) THEN SUM(net) END) 'wtd',
        (CASE WHEN DATE_FORMAT(trading_createAt, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m') THEN SUM(net) END) 'mtd'
        FROM daily_2020
        WHERE DATE_FORMAT(trading_createAt, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m')
        GROUP BY product_name, company, MONTH(trading_createAt)`;

        // TODO: DONE
        var queryProductPerCompanyYtd2020 = `
        SELECT product_name, company, 
        CONCAT((SUM(IF(trading_createAt <= DATE(CURDATE()), net, 0)) * 100 / (SELECT SUM(IF(trading_createAt <= DATE(CURDATE()), net, 0)) AS s FROM daily_2020)),'%') AS 'partition',
        (CASE WHEN YEAR(trading_createAt) = YEAR(CURDATE()) AND trading_createAt <= LAST_DAY(CURDATE()) THEN SUM(net) END) 'ytd'
        FROM daily_2020
        GROUP BY product_name, company, YEAR(trading_createAt)`;

        // TODO: DONE
        var queryProductPerCompanyYtd2019 = `
        SELECT  product_name, company, 
	(CASE WHEN YEAR(trading_createAt) = YEAR(CURDATE() - INTERVAL 1 YEAR) THEN SUM(net) END) 'full_2019',
	(CASE WHEN DATE(trading_createAt) BETWEEN '2019-01-01 00:00:00' AND DATE(CURDATE() - INTERVAL 1 YEAR) THEN SUM(net) END) 'ytd_2019'
        FROM daily_2019
        GROUP BY product_name, company`;

        // var queryProductPerCompany = `
        // SELECT product_name, company, 
        // concat(round((SUM(${filter.requestedPnl}_2020) / (SELECT SUM(${filter.requestedPnl}_2019) AS s FROM daily_full) *100  ),2),'%') AS 'variation',
        // (CASE WHEN YEAR(trading_createAt) = YEAR(${filter.chosenDate}) THEN SUM(${filter.requestedPnl}_2020) * 100 / (SELECT SUM(${filter.requestedPnl}_2020) AS s FROM daily_full) END) AS 'partition',
        // (CASE WHEN trading_createAt >= DATE(${filter.chosenDate}) THEN SUM(${filter.requestedPnl}_2020) END) todayPnl,
        // (CASE WHEN WEEK(trading_createAt) = WEEK(${filter.chosenDate}) THEN SUM(${filter.requestedPnl}_2020) END) wtd,
        // (CASE WHEN MONTH(trading_createAt) = MONTH(${filter.chosenDate}) THEN SUM(${filter.requestedPnl}_2020) END) mtd,
        // (CASE WHEN YEAR(trading_createAt) = YEAR(${filter.chosenDate}) THEN SUM(${filter.requestedPnl}_2020) END) ytd,
        // (CASE WHEN DATE(trading_createAt) BETWEEN '2019-01-01 00:00:00' AND DATE(${filter.chosenDate}) - INTERVAL 1 YEAR THEN SUM(${filter.requestedPnl}_2019) END) 'ytd_2019',
        // (CASE WHEN YEAR(trading_createAt) = YEAR(DATE(${filter.chosenDate}) - INTERVAL 1 YEAR) THEN SUM(${filter.requestedPnl}_2019) END) 'full_2019'
        // FROM daily_full
        // GROUP BY product_name, company, YEAR(trading_createAt)`;

        let productPerCompanyMonthly2020 = await DBService.runSQL(queryProductPerCompanyMonthly2020);
        let productPerCompanyYtd2020 = await DBService.runSQL(queryProductPerCompanyYtd2020);
        let productPerCompanyYtd2019 = await DBService.runSQL(queryProductPerCompanyYtd2019);

        // TODO: check why weekly results not received
        var queryProductMonthlyTotals2020 = `
        SELECT product_name,
	(CASE WHEN trading_createAt >= DATE(CURDATE()) THEN SUM(net) END) 'todayPnl',
        (CASE WHEN WEEK(trading_createAt) = WEEK(CURDATE()) THEN SUM(net) END) 'wtd',
        (CASE WHEN DATE_FORMAT(trading_createAt, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m') THEN SUM(net) END) 'mtd'
        FROM daily_2020
        WHERE DATE_FORMAT(trading_createAt, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m')
        GROUP BY product_name, MONTH(trading_createAt)`;

        var queryProductYtdTotals2020 = `
        SELECT product_name,
	(CASE WHEN YEAR(trading_createAt) = YEAR(CURDATE()) THEN SUM(net) END) ytd
        FROM daily_2020
        GROUP BY product_name`;

        var queryProductYtdTotals2019 = `
        SELECT  product_name,
	(CASE WHEN YEAR(trading_createAt) = YEAR(CURDATE() - INTERVAL 1 YEAR) THEN SUM(net) END) 'full_2019',
	(CASE WHEN DATE(trading_createAt) BETWEEN '2019-01-01 00:00:00' AND DATE(CURDATE() - INTERVAL 1 YEAR) THEN SUM(net) END) 'ytd_2019'
        FROM daily_2019
        GROUP BY product_name`;

        let productMonthlyTotals2020 = await DBService.runSQL(queryProductMonthlyTotals2020);
        let productYtdTotals2020 = await DBService.runSQL(queryProductYtdTotals2020);
        let productYtdTotals2019 = await DBService.runSQL(queryProductYtdTotals2019);

        // TODO: check last year sum does not received by company only - need to use 2 table to get this. issue with that: can't get variance here, need to add manually
        // var queryCompanyTotals = `
        // SELECT company, 
        // (CASE WHEN YEAR(trading_createAt) = YEAR(CURDATE()) THEN SUM(net_2020) * 100 / (SELECT SUM(net_2020) AS s FROM daily_full) END) AS 'partition',
        // (CASE WHEN trading_createAt >= DATE(NOW()) THEN SUM(net_2020) END) todayPnl,
        // (CASE WHEN WEEK(trading_createAt) = WEEK(CURDATE()) THEN SUM(net_2020) END) wtd,
        // (CASE WHEN MONTH(trading_createAt) = MONTH(CURDATE()) THEN SUM(net_2020) END) mtd,
        // (CASE WHEN YEAR(trading_createAt) = YEAR(CURDATE()) THEN SUM(net_2020) END) ytd,
        // (CASE WHEN DATE(trading_createAt)  BETWEEN '2019-01-01 00:00:00' AND DATE(CURDATE()) - INTERVAL 1 YEAR THEN SUM(net_2019) END) 'ytd_2019',
        // (CASE WHEN YEAR(trading_createAt) = YEAR(DATE(CURDATE()) - INTERVAL 1 YEAR) THEN SUM(net_2019) END) 'full_2019'
        // FROM daily_full
        // GROUP BY company, YEAR(trading_createAt)`;

        var queryCompanyDayWeekTotals2020 = `
        SELECT company, 
	(CASE WHEN trading_createAt >= DATE(CURDATE()) THEN SUM(net) END) todayPnl,
	(CASE WHEN WEEK(trading_createAt) = WEEK(CURDATE()) THEN SUM(net) END) wtd
        FROM daily_2020
        GROUP BY company, WEEK(trading_createAt)`;

        // TODO: check WTD here
        var queryCompanyMonthlyTotals2020 = `
        SELECT  company, 
        (CASE WHEN DATE_FORMAT(trading_createAt, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m') THEN SUM(net) END) 'mtd'
        FROM daily_2020
        GROUP BY company, MONTH(trading_createAt)`;

        var queryCompanyYtdTotals2020 = `
        SELECT company, 
        (CASE WHEN YEAR(trading_createAt) = YEAR(CURDATE()) THEN SUM(net) END) ytd
        FROM daily_2020
        GROUP BY company`;

        let companyDayWeekTotals2020 = await DBService.runSQL(queryCompanyDayWeekTotals2020);
        let companyMonthlyTotals2020 = await DBService.runSQL(queryCompanyMonthlyTotals2020);
        let companyYtdTotals2020 = await DBService.runSQL(queryCompanyYtdTotals2020);

        // var queryCompanyTotals2019 = `
        // SELECT company,
        // (CASE WHEN DATE(trading_createAt)  BETWEEN '2019-01-01 00:00:00' AND DATE(CURDATE()) - INTERVAL 1 YEAR THEN SUM(${filter.requestedPnl}) END) 'ytd_2019',
        // (CASE WHEN YEAR(trading_createAt) = YEAR(DATE(CURDATE()) - INTERVAL 1 YEAR) THEN SUM(${filter.requestedPnl}) END) 'full_2019'
        // FROM daily_2019
        // GROUP BY company`;
        // let companyTotals2019 = await DBService.runSQL(queryCompanyTotals2019);

        // TODO: check last year sum does not received by company only - need to use 2 table to get this. issue with that: can't get variance here, need to add manually
        var queryFinalTotalsWtd = `
        SELECT SUM(net), 
	(CASE WHEN trading_createAt >= DATE(NOW()) THEN SUM(net) END) todayPnl,
        (CASE WHEN WEEK(trading_createAt) = WEEK(CURDATE()) THEN SUM(net) END) wtd
        FROM daily_2020
        GROUP BY WEEK(trading_createAt)`;

        let finalWtdTotals2020res = await DBService.runSQL(queryFinalTotalsWtd);

        let finalWtdTotals2020 = {
                today: 0,
                wtd: 0
        }

        finalWtdTotals2020res.forEach((row, i) => {
                if (row.todayPnl) finalWtdTotals2020.today += row.todayPnl;
                if (row.wtd) finalWtdTotals2020.wtd += row.wtd;
        });

        // console.log('finalWtdTotals2020: ', finalWtdTotals2020);

        var queryFinalTotalsMtd2020 = `
        SELECT SUM(net), 
        (CASE WHEN MONTH(trading_createAt) = MONTH(CURDATE()) THEN SUM(net) END) mtd
        FROM daily_2020
        GROUP BY MONTH(trading_createAt)`;

        let finalMtdTotals2020 = await DBService.runSQL(queryFinalTotalsMtd2020);

        var queryFinalTotalsYtd2020 = `
        SELECT sum(net),
    	(CASE WHEN YEAR(trading_createAt) = YEAR(CURDATE()) THEN SUM(net) END) ytd
        FROM daily_2020
        GROUP BY YEAR(trading_createAt)`;

        let finalYtdTotals2020 = await DBService.runSQL(queryFinalTotalsYtd2020);

        // need 'GROUP BY company' to get ytd_2019 results.
        var queryFinalTotals2019 = `
        SELECT SUM(net),
	(CASE WHEN YEAR(trading_createAt) = YEAR(CURDATE() - INTERVAL 1 YEAR) THEN SUM(net) END) 'full_2019',
	(CASE WHEN DATE(trading_createAt) BETWEEN '2019-01-01 00:00:00' AND DATE(CURDATE() - INTERVAL 1 YEAR) THEN SUM(net) END) 'ytd_2019'
        FROM daily_2019
        GROUP BY company`;

        let finalTotals2019res = await DBService.runSQL(queryFinalTotals2019);
        let finalTotals2019 = {
                full_2019: 0,
                ytd_2019: 0
        }

        finalTotals2019res.forEach((row, i) => {
                if (row.full_2019) finalTotals2019.full_2019 += row.full_2019;
                if (row.ytd_2019) finalTotals2019.ytd_2019 += row.ytd_2019;
        });

        const result = {
                productPerCompanyMonthly2020,
                productPerCompanyYtd2020,
                productPerCompanyYtd2019,

                productMonthlyTotals2020,
                productYtdTotals2020,
                productYtdTotals2019,

                companyDayWeekTotals2020,
                companyMonthlyTotals2020,
                companyYtdTotals2020,

                finalMtdTotals2020: finalMtdTotals2020[0],
                finalYtdTotals2020,
                finalTotals2019
        }

        return result;
}

async function _addToDaily2020Table() {
        var query = `
        INSERT INTO daily_2020
	(company, desk, team, employee, 
        trading_id, trading_createAt , client_id, client_name, 
        product_id, product_name, net, gross)
    
        SELECT com.company_name AS 'company', d.desk_name AS 'desk', te.team_name AS 'team', e.employee_name AS 'employee',
	tr.id AS 'trading_id', tr.createAt AS 'trading_createAt',  tr.client_id, cl.client_name AS 'client', 
        tr.product_id, p.product_name AS 'product', tr.pnl_net AS 'net', tr.pnl_gross AS 'gross'
	FROM trading tr 
        INNER JOIN client cl ON cl.id = tr.client_id
        INNER JOIN employee e ON e.id = cl.employee_id
        INNER JOIN product p ON p.id = tr.product_id
        INNER JOIN desk d ON d.id = e.desk_id
        INNER JOIN team te ON te.id = e.team_id
        INNER JOIN company com ON com.id = d.company_id
        WHERE YEAR(tr.createAt) = YEAR((CURDATE()))
        `
        let results = await DBService.runSQL(query);
        return results;
}

async function _addToDaily2019Table() {
        var query = `
        INSERT INTO daily_2019
	(company, desk, team, employee, 
        trading_id, trading_createAt , client_id, client_name, 
        product_id, product_name, net, gross)
    
        SELECT com.company_name AS 'company', d.desk_name AS 'desk', te.team_name AS 'team', e.employee_name AS 'employee',
	tr.id AS 'trading_id', tr.createAt AS 'trading_createAt',  tr.client_id, cl.client_name AS 'client', 
        tr.product_id, p.product_name AS 'product', tr.pnl_net AS 'net', tr.pnl_gross AS 'gross'
	FROM trading tr 
        INNER JOIN client cl ON cl.id = tr.client_id
        INNER JOIN employee e ON e.id = cl.employee_id
        INNER JOIN product p ON p.id = tr.product_id
        INNER JOIN desk d ON d.id = e.desk_id
        INNER JOIN team te ON te.id = e.team_id
        INNER JOIN company com ON com.id = d.company_id
        WHERE YEAR(tr.createAt) = YEAR(CURDATE() - INTERVAL 1 YEAR)
        `
        let results = await DBService.runSQL(query);
        return results;
}

async function _addToDailyFullTable() {
        var query = `
        INSERT INTO daily_full
	(company, desk, team, employee, 
        trading_id, trading_createAt , client_id, client_name, 
        product_id, product_name, net_2019, gross_2019)
    
        SELECT com.company_name AS 'company', d.desk_name AS 'desk', te.team_name AS 'team', e.employee_name AS 'employee',
	tr.id AS 'trading_id', tr.createAt AS 'trading_createAt',  tr.client_id, cl.client_name AS 'client', 
        tr.product_id, p.product_name AS 'product', tr.pnl_net AS 'net_2019', tr.pnl_gross AS 'gross_2019'
	FROM trading tr 
        INNER JOIN client cl ON cl.id = tr.client_id
        INNER JOIN employee e ON e.id = cl.employee_id
        INNER JOIN product p ON p.id = tr.product_id
        INNER JOIN desk d ON d.id = e.desk_id
        INNER JOIN team te ON te.id = e.team_id
        INNER JOIN company com ON com.id = d.company_id
        WHERE YEAR(tr.createAt) = YEAR(CURDATE() - INTERVAL 1 YEAR)
        `
        let results = await DBService.runSQL(query);
        return results;
}

module.exports = {
        queryDailyView
}