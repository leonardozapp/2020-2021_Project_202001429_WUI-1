<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\SituacaoFundo $situacaoFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $situacaoFundo->cnpj_fundo_id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $situacaoFundo->cnpj_fundo_id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Situacao Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="situacaoFundos form content">
            <?= $this->Form->create($situacaoFundo) ?>
            <fieldset>
                <legend><?= __('Edit Situacao Fundo') ?></legend>
                <?php
                    echo $this->Form->control('DT_REG_CVM');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
