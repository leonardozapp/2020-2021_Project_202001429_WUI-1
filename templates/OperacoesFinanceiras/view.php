<?php

/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\OperacoesFinanceira $operacoesFinanceira
 */
?>
<div class="row">
    <div class="column-responsive column-100">
        <div class="operacoesFinanceiras view content">
            <h3>Operação: <?= h($operacoesFinanceira->id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Carteiras Investimento') ?></th>
                    <td><?= $operacoesFinanceira->has('carteiras_investimento') ? $this->Html->link($operacoesFinanceira->carteiras_investimento->id, ['controller' => 'CarteirasInvestimentos', 'action' => 'view', $operacoesFinanceira->carteiras_investimento->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Cnpj Fundo') ?></th>
                    <td><?= $operacoesFinanceira->has('cnpj_fundo') ? $this->Html->link($operacoesFinanceira->cnpj_fundo->id, ['controller' => 'CnpjFundos', 'action' => 'view', $operacoesFinanceira->cnpj_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Distribuidor Fundo') ?></th>
                    <td><?= $operacoesFinanceira->has('distribuidor_fundo') ? $this->Html->link($operacoesFinanceira->distribuidor_fundo->id, ['controller' => 'DistribuidorFundos', 'action' => 'view', $operacoesFinanceira->distribuidor_fundo->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Tipo Operacoes Financeira') ?></th>
                    <td><?= $operacoesFinanceira->has('tipo_operacoes_financeira') ? $this->Html->link($operacoesFinanceira->tipo_operacoes_financeira->nome, ['controller' => 'TipoOperacoesFinanceiras', 'action' => 'view', $operacoesFinanceira->tipo_operacoes_financeira->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Id') ?></th>
                    <td><?= $this->Number->format($operacoesFinanceira->id) ?></td>
                </tr>
                <tr>
                    <th><?= __('Valor Total') ?></th>
                    <td><?= $this->Number->format($operacoesFinanceira->valor_total) ?></td>
                </tr>
                <tr>
                    <th><?= __('Valor Cota') ?></th>
                    <td><?= $this->Number->format($operacoesFinanceira->valor_cota) ?></td>
                </tr>
                <tr>
                    <th><?= __('Quantidade Cotas') ?></th>
                    <td><?= $this->Number->format($operacoesFinanceira->quantidade_cotas) ?></td>
                </tr>
                <tr>
                    <th><?= __('Data') ?></th>
                    <td><?= h($operacoesFinanceira->data) ?></td>
                </tr>
                <tr>
                    <th><?= __('Por Valor') ?></th>
                    <td><?= $operacoesFinanceira->por_valor ? __('Yes') : __('No'); ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>